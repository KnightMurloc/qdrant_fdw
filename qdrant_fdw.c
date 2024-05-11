#include <postgres.h>

#include "cJSON/cJSON.h"
#include "filter.h"
#include <access/table.h>
#include <catalog/pg_proc.h>
#include <commands/defrem.h>
#include <commands/explain.h>
#include <curl/curl.h>
#include <fmgr.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/planmain.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/ruleutils.h>
#include <utils/syscache.h>

#define IS_VECTOR(attno) \
    (strcmp((const char*) &desc->attrs[attno - 1].attname, "vector") == 0)

PG_MODULE_MAGIC;
extern PGDLLIMPORT int work_mem;

typedef struct QdrantFdwRelationInfo {
	ForeignTable* table;
	ForeignServer* server;

	RelOptInfo* order_rel;
	bool can_push_down;
} QdrantFdwRelationInfo;

typedef enum {
	QUERY_SCROLL,
	QUERY_SEARCH
} QdrantQueryType;

typedef struct State {
	CURL* curl;
	Tuplestorestate* ts;
	int pointer;

	List* target_names;

	char* response;
	Size response_size;
	TupleTableSlot* state_slot;
	cJSON* query;
	List* arguments;
	
	//for explain only
	StringInfo url;
} State;

typedef struct QdrantPlan {
	List* arguments;
	List* target_names;
	cJSON* query;
	QdrantQueryType queryType;

	ForeignTable* table;
	ForeignServer* server;
} QdrantPlan;

typedef struct QdrantAttribute {
	char name[NAMEDATALEN];
	int index;
	Oid type;
} QdrantAttribute;

static void QdrantGetForeignRelSize(PlannerInfo* root,
									RelOptInfo* baserel,
									Oid foreigntableid);

static void QdrantGetForeignPaths(PlannerInfo* root,
								  RelOptInfo* baserel,
								  Oid foreigntableid);

static ForeignScan* QdrantGetForeignPlan(PlannerInfo* root,
										 RelOptInfo* foreignrel,
										 Oid foreigntableid,
										 ForeignPath* best_path,
										 List* tlist,
										 List* scan_clauses,
										 Plan* outer_plan);

static void QdrantExplainForeignScan(ForeignScanState* node,
									 ExplainState* es);

static void QdrantBeginForeignScan(ForeignScanState* node, int eflags);

static TupleTableSlot* QdrantIterateForeignScan(ForeignScanState* node);

static void QdrantGetForeignUpperPaths(PlannerInfo* root,
									   UpperRelationKind stage,
									   RelOptInfo* input_rel,
									   RelOptInfo* output_rel,
									   void* extra);

static void QdrantReScanForeignScan(ForeignScanState* node);

static void QdrantEndForeignScan(ForeignScanState* node);

static bool is_sim_func(Oid funcoid) {
	HeapTuple tuple = SearchSysCache1(PROCOID, funcoid);
	bool result = true;

	bool is_null;
	Datum probin = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_probin,
								   &is_null);

	if(is_null) {
		result = false;
		goto end;
	}

	if(strstr(DatumGetCString(probin), "qdrant_fdw") == NULL) {
		result = false;
		goto end;
	}

	Datum proname = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proname,
									&is_null);
	Assert(!is_null);

	if(strcmp("sim", DatumGetCString(proname)) != 0) {
		result = false;
		goto end;
	}

	end:
	ReleaseSysCache(tuple);
	return result;
}

PG_FUNCTION_INFO_V1(sim);

Datum sim(PG_FUNCTION_ARGS) {

	//does not anything shouldn't be executed 

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(qdrant_fdw_handler);

static void* pcalloc(size_t num, size_t size) {
	return palloc(num * size);
}

Datum
qdrant_fdw_handler(PG_FUNCTION_ARGS) {
//postgres_fdw_handler(PG_FUNCTION_ARGS) {
	FdwRoutine* routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = QdrantGetForeignRelSize;
	routine->GetForeignPaths = QdrantGetForeignPaths;
	routine->GetForeignPlan = QdrantGetForeignPlan;
	routine->ExplainForeignScan = QdrantExplainForeignScan;
	routine->BeginForeignScan = QdrantBeginForeignScan;
	routine->IterateForeignScan = QdrantIterateForeignScan;
	routine->ReScanForeignScan = QdrantReScanForeignScan;
	routine->EndForeignScan = QdrantEndForeignScan;
	routine->GetForeignUpperPaths = QdrantGetForeignUpperPaths;

	curl_global_init(0);
	curl_global_init_mem(0, palloc, pfree, repalloc, pstrdup, pcalloc);

	PG_RETURN_POINTER(routine);
}

static void
QdrantGetForeignRelSize(PlannerInfo* root,
						RelOptInfo* baserel,
						Oid foreigntableid) {
	QdrantFdwRelationInfo* fpinfo;
	fpinfo = (QdrantFdwRelationInfo*) palloc0(sizeof(QdrantFdwRelationInfo));
	baserel->fdw_private = (void*) fpinfo;

	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	baserel->rows = 1;
	baserel->tuples = 1;
	baserel->reltarget->width = 1;
}

static void
QdrantGetForeignPaths(PlannerInfo* root,
					  RelOptInfo* baserel,
					  Oid foreigntableid) {
	bool can_pushdown = baserel->relids->nwords == 1;
	if(baserel->baserestrictinfo) {
		can_pushdown &= list_length(baserel->baserestrictinfo);
	}

	if(can_pushdown && baserel->baserestrictinfo) {
		Relation rel = table_open(foreigntableid, NoLock);
		TupleDesc desc = RelationGetDescr(rel);
		RestrictInfo* restrictInfo = linitial(baserel->baserestrictinfo);
		can_pushdown = can_pushdown_filter(desc, restrictInfo->clause);
		table_close(rel, NoLock);
	}

	((QdrantFdwRelationInfo*) baserel->fdw_private)->can_push_down = can_pushdown;

	Path* path = (Path*) create_foreignscan_path(root, baserel,
										 NULL,    /* default pathtarget */
										 baserel->rows,
										 1,
										 1,
										 NIL, /* no pathkeys */
										 baserel->lateral_relids,
										 NULL, /* no fdw_restrictinfo list */
										 NIL);
	add_path(baserel, (Path*) path);
}

static void QdrantGetForeignUpperPaths(PlannerInfo* root,
									   UpperRelationKind stage,
									   RelOptInfo* input_rel,
									   RelOptInfo* output_rel,
									   void* extra) {
	if((stage != UPPERREL_ORDERED &&
		stage != UPPERREL_FINAL) ||
	   output_rel->fdw_private)
		return;

	if(stage == UPPERREL_ORDERED) {
		if(input_rel->fdw_private == NULL || !((QdrantFdwRelationInfo*) input_rel->fdw_private)->can_push_down) {
			return;
		}

		if(list_length(root->query_pathkeys) != 1) {
			elog(NOTICE,
				 "cannot pushdown ORDER BY with more that one attribute");
			return;
		}

		PathKey* key = linitial(root->query_pathkeys);
		EquivalenceClass* class = key->pk_eclass;
		if(class->ec_members->length != 1) {
			elog(NOTICE,
				 "cannot pushdown ORDER BY with more that one attribute");
			return;
		}

		EquivalenceMember* member = linitial(class->ec_members);

		if(!IsA(member->em_expr, FuncExpr)) {
			elog(NOTICE, "can only push down ORDER BY with sim function");
			return;
		}

		FuncExpr* expr = (FuncExpr*) member->em_expr;

		if(!is_sim_func(expr->funcid)) {
			elog(NOTICE, "can only push down ORDER BY with sim function");
			return;
		}

		if(!IsA(list_nth(expr->args, 0), Var)) {
			elog(NOTICE, "cannot pushdown ORDER BY with modified vector");
			return;
		}

		if(root->parse->limitCount == NULL) {
			elog(ERROR, "cannot pushdown ORDER BY without limit");
			return;
		}
		
		output_rel->fdw_private = input_rel->fdw_private;
		((QdrantFdwRelationInfo*) output_rel->fdw_private)->order_rel = input_rel;
		Path* ordered_path = (Path*) create_foreign_upper_path(root,
															   input_rel,
															   root->upper_targets[UPPERREL_ORDERED],
															   1,
															   1,
															   1,
															   root->sort_pathkeys,
															   NULL,    /* no fdw_restrictinfo list */
															   list_make2_int(
																	   true,
																	   false));
		add_path(output_rel, ordered_path);
		return;
	}

	if(stage == UPPERREL_FINAL) {

		QdrantFdwRelationInfo* relationInfo = input_rel->fdw_private;
		if(input_rel->fdw_private == NULL || !relationInfo->can_push_down) {
			return;
		}

		if(root->parse->limitCount == NULL ||
		   root->parse->limitCount->type != T_Const) {
			return;
		}

		bool has_order = false;
		if(input_rel->reloptkind == RELOPT_UPPER_REL &&
		   relationInfo->order_rel != NULL) {
			input_rel = relationInfo->order_rel;
			has_order = true;
		}
		output_rel->fdw_private = input_rel->fdw_private;

		Path* final_path = (Path*) create_foreign_upper_path(root,
															 input_rel,
															 root->upper_targets[UPPERREL_FINAL],
															 0,
															 0,
															 0,
															 NIL,
															 NULL, /* no fdw_restrictinfo list */
															 list_make2_int(
																	 has_order,
																	 true));

		/* and add it to the final_rel */
		add_path(output_rel, (Path*) final_path);
		return;
	}
}

static ForeignScan* QdrantGetForeignPlan(PlannerInfo* root,
										 RelOptInfo* foreignrel,
										 Oid foreigntableid,
										 ForeignPath* best_path,
										 List* tlist,
										 List* scan_clauses,
										 Plan* outer_plan) {

	bool has_order_by;
	bool has_limit;

	List* fdw_exprs = NIL;
	List* local_exprs = NIL;
	ListCell* lc;
	QdrantPlan* plan = palloc0(sizeof(QdrantPlan));
	QdrantFdwRelationInfo* info = foreignrel->fdw_private;

	bool isvarlena;
	Relation rel = table_open(info->table->relid, NoLock);
	TupleDesc desc = RelationGetDescr(rel);
	plan->query = cJSON_CreateObject();
	plan->queryType = QUERY_SCROLL;
	plan->table = info->table;
	plan->server = info->server;
	if(best_path->fdw_private) {
		has_order_by = list_nth_int(best_path->fdw_private, 0);
		has_limit = list_nth_int(best_path->fdw_private, 1);

		if(has_order_by) {
			Expr* order_by_expr = NULL;
			PathKey* key = linitial(root->query_pathkeys);
			EquivalenceClass* class = key->pk_eclass;

			EquivalenceMember* member = linitial(class->ec_members);
			FuncExpr* expr = (FuncExpr*) member->em_expr;
			Var* vector_var = list_nth(expr->args, 0);

			if(!IS_VECTOR(vector_var->varattno)) {
				elog(ERROR, "can push down only order by vector");
			}

			order_by_expr = list_nth(expr->args, 1);
			fdw_exprs = lappend(fdw_exprs, order_by_expr);

			Argument* argument = palloc(sizeof(Argument));
			argument->expr = order_by_expr;
			argument->value_ref = cJSON_CreateNull();
			plan->arguments = lappend(plan->arguments, argument);
			cJSON_AddItemToObject(plan->query, "vector", argument->value_ref);
			plan->queryType = QUERY_SEARCH;
			getTypeOutputInfo(exprType( (Node*) order_by_expr), &argument->type,
							  &isvarlena);
		}

		if(has_limit) {
			Expr* limit_expr = (Expr*) root->parse->limitCount;
			fdw_exprs = lappend(fdw_exprs, limit_expr);

			Argument* argument_limit = palloc(sizeof(Argument));
			argument_limit->expr = limit_expr;
			argument_limit->value_ref = cJSON_CreateNull();
			plan->arguments = lappend(plan->arguments, argument_limit);
			cJSON_AddItemToObject(plan->query, "limit", argument_limit->value_ref);
			getTypeOutputInfo(exprType((Node*) limit_expr), &argument_limit->type,
							  &isvarlena);
			
			Expr* offset_expr = (Expr*) root->parse->limitOffset;
			if(offset_expr){
				fdw_exprs = lappend(fdw_exprs, offset_expr);
				Argument* argument_offset = palloc(sizeof(Argument));
				argument_offset->expr = offset_expr;
				argument_offset->value_ref = cJSON_CreateNull();
				plan->arguments = lappend(plan->arguments, argument_offset);
				cJSON_AddItemToObject(plan->query, "offset", argument_offset->value_ref);
				getTypeOutputInfo(exprType((Node*) offset_expr), &argument_offset->type,
								  &isvarlena);
			}
		}
	}

	AttributeType attribute_used = 0;

	cJSON* filter = NULL;
	foreach(lc, scan_clauses) {
		List* filter_arguments = NIL;
		
		RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);

		if(IsA(rinfo->clause, NullTest) || !info->can_push_down) {
			local_exprs = lappend(local_exprs, rinfo->clause);
			continue;
		}
		filter = build_filter(desc, &filter_arguments, rinfo->clause,
							  &attribute_used);
		if(list_length(filter_arguments) == 1) {
			cJSON* new_root = cJSON_CreateObject();
			cJSON* must = cJSON_CreateArray();
			cJSON_AddItemToArray(must, filter);
			cJSON_AddItemToObject(new_root, "must", must);
			filter = new_root;
		}
		
		ListCell* lcc;
		foreach(lcc, filter_arguments) {
			Argument* argument = lfirst_node(Argument, lcc);
			fdw_exprs = lappend(fdw_exprs, argument->expr);
		}
		plan->arguments = list_concat(plan->arguments, filter_arguments);
	}
	cJSON_AddItemToObject(plan->query, "filter", filter);
	List* fetch_tlist = pull_var_clause((Node*) foreignrel->reltarget->exprs,
										PVC_RECURSE_PLACEHOLDERS);

	foreach(lc, fetch_tlist) {
		Var* target = lfirst_node(Var, lc);

		if(IS_VECTOR(target->varattno)) {
			attribute_used |= HAS_VECTOR;
		}else if(strcmp(
				(const char*) &desc->attrs[target->varattno - 1].attname,
				"id") != 0) {
			attribute_used |= HAS_PAYLOAD;
		}

		QdrantAttribute* attribute = palloc(sizeof(QdrantAttribute));
		memcpy(attribute->name, &desc->attrs[target->varattno - 1].attname,
			   sizeof(attribute->name));
		attribute->index = target->varattno - 1;
		attribute->type = target->vartype;
		plan->target_names = lappend(plan->target_names, attribute);
	}
	list_free(fetch_tlist);

	if(attribute_used & HAS_VECTOR) {
		cJSON_AddItemToObject(plan->query, "with_vector", cJSON_CreateTrue());
	}else {
		cJSON_AddItemToObject(plan->query, "with_vector", cJSON_CreateFalse());
	}

	if(attribute_used & HAS_PAYLOAD) {
		cJSON_AddItemToObject(plan->query, "with_payload", cJSON_CreateTrue());
	}else {
		cJSON_AddItemToObject(plan->query, "with_payload", cJSON_CreateFalse());
	}

	ForeignScan* scan = make_foreignscan(tlist,
										 local_exprs,
										 foreignrel->relid,
										 fdw_exprs,
										 list_make1(plan),
										 NIL,
										 NIL,
										 outer_plan);
	table_close(rel, NoLock);
	return scan;
}

static inline StringInfo build_url(ForeignServer* server, ForeignTable* table, QdrantQueryType type){
	StringInfo result = makeStringInfo();
	const char* host = NULL;
	const char* port = NULL;
	const char* collection = NULL;
	bool use_https = false;
	ListCell* lc;
	foreach(lc, server->options){
		DefElem* def = lfirst_node(DefElem, lc);
		if(strcmp(def->defname, "host") == 0){
			host = defGetString(def);
			continue;
		}

		if(strcmp(def->defname, "port") == 0){
			port = defGetString(def);
			continue;
		}

		if(strcmp(def->defname, "use_https") == 0){
			use_https = defGetBoolean(def);
			continue;
		}

		elog(WARNING, "unknown option: %s", def->defname);
	}

	foreach(lc, table->options){
		DefElem* def = lfirst_node(DefElem, lc);
		if(strcmp(def->defname, "collection") == 0){
			collection = defGetString(def);
			continue;
		}
	}

	appendStringInfo(result, "http%s://%s:%s/collections/%s/points/%s",
					 use_https ? "s" : "", host,
					 port ? port : "6333",
					 collection,
					 type == QUERY_SCROLL ? "scroll" : "search");
	return result;
}

static void QdrantExplainForeignScan(ForeignScanState* node,
									 ExplainState* es) {
	State* state = node->fdw_state;
	
	ExplainPropertyText("POST", state->url->data, es);

	ListCell* lc;
	foreach(lc, state->arguments) {

		List* context = set_deparse_context_plan(es->deparse_cxt,
												 node->ss.ps.plan,
												 es->rtable);

		Argument* argument = lfirst_node(Argument, lc);
		char* value = deparse_expression((Node*) argument->expr, context, false,
										 false);

		argument->value_ref->valuestring = value;
		argument->value_ref->type = cJSON_Raw;
	}

	const char* debug_query = cJSON_Print(state->query);
	ExplainPropertyText("", pstrdup(debug_query), es);
}

static size_t writefunc(char* ptr, size_t size, size_t nmemb, State* state) {
	size_t new_len = state->response_size + size * nmemb;
	if(state->response) {
		state->response = repalloc(state->response, new_len + 1);
	}else {
		state->response = palloc(new_len);
	}
	if(state->response == NULL) {
		elog(ERROR, "repalloc failed");
	}
	memcpy(state->response + state->response_size, ptr, size * nmemb);
	state->response[new_len] = '\0';
	state->response_size = new_len;


	return size * nmemb;
}

static void
QdrantBeginForeignScan(ForeignScanState* node, int eflags) {
	ForeignScan* fsplan = (ForeignScan*) node->ss.ps.plan;
	State* state = palloc0(sizeof(State));

	node->fdw_state = state;

	state->curl = curl_easy_init();

	struct curl_slist* hs = NULL;
	hs = curl_slist_append(hs, "Content-Type: application/json");
	curl_easy_setopt(state->curl, CURLOPT_HTTPHEADER, hs);
	curl_easy_setopt(state->curl, CURLOPT_POST, 1);
	curl_easy_setopt(state->curl, CURLOPT_WRITEFUNCTION, writefunc);
	curl_easy_setopt(state->curl, CURLOPT_WRITEDATA, node->fdw_state);

	state->state_slot = ExecInitExtraTupleSlot(node->ss.ps.state,
											   node->ss.ss_ScanTupleSlot->tts_tupleDescriptor,
											   &TTSOpsMinimalTuple);

	QdrantPlan* plan = linitial(fsplan->fdw_private);
	state->target_names = plan->target_names;
	ListCell* lc_expr;
	ListCell* lc_argument;
	forboth(lc_expr, fsplan->fdw_exprs, lc_argument, plan->arguments) {
		Argument* argument = lfirst_node(Argument, lc_argument);
		
		argument->state = ExecInitExpr(lfirst(lc_expr), &node->ss.ps);
	}
	state->arguments = plan->arguments;
	state->query = plan->query;

	StringInfo url = build_url(plan->server, plan->table, plan->queryType);

	state->url = url;
	
	curl_easy_setopt(state->curl, CURLOPT_URL, url->data);
}

static inline cJSON* build_array(Datum array_datum, Oid array_type) {

	bool is_null;

	cJSON* array = cJSON_CreateArray();

	ArrayType* arrayType = DatumGetArrayTypeP(array_datum);
	int ndim = ARR_NDIM(arrayType);
	int* dims = ARR_DIMS(arrayType);
	int* lbs = ARR_LBOUND(arrayType);

	//get arraytyplen for array 
	HeapTuple type_tuple;
	type_tuple = SearchSysCache1(TYPEOID, array_type);
	int arraytyplen = DatumGetInt32(
			SysCacheGetAttr(TYPEOID, type_tuple, Anum_pg_type_typlen,
							&is_null));
	ReleaseSysCache(type_tuple);

	//get info for array element
	type_tuple = SearchSysCache1(TYPEOID, arrayType->elemtype);
	int typelen = DatumGetInt32(
			SysCacheGetAttr(TYPEOID, type_tuple, Anum_pg_type_typlen,
							&is_null));
	bool typbyval = DatumGetBool(
			SysCacheGetAttr(TYPEOID, type_tuple, Anum_pg_type_typbyval,
							&is_null));
	char typalign = DatumGetChar(
			SysCacheGetAttr(TYPEOID, type_tuple, Anum_pg_type_typalign,
							&is_null));
	ReleaseSysCache(type_tuple);
	for(int i = lbs[0]; i < dims[0] + lbs[0]; i++) {
		is_null = false;
		Datum elem = array_get_element(array_datum, 1, &i, arraytyplen, typelen,
									   typbyval, typalign, &is_null);
		if(is_null) {
			cJSON_AddItemToArray(array, cJSON_CreateNull());
			continue;
		}
		switch(arrayType->elemtype) {
			case INT2OID:
			case INT4OID:
			case INT8OID:
				cJSON_AddItemToArray(array, cJSON_CreateNumber(
						(double) DatumGetInt64(elem)));
				break;
			case FLOAT4OID:
				cJSON_AddItemToArray(array, cJSON_CreateNumber(
						(double) DatumGetFloat4(elem)));
				break;
			case FLOAT8OID:
				cJSON_AddItemToArray(array,
									 cJSON_CreateNumber(DatumGetFloat8(elem)));
				break;
			case TEXTOID:
				cJSON_AddItemToArray(array, cJSON_CreateString(
						TextDatumGetCString(elem)));
				break;
			default:
				elog(ERROR, "unsupported array element type");
		}
	}

	return array;

}

static TupleTableSlot*
QdrantIterateForeignScan(ForeignScanState* node) {
	State* state = node->fdw_state;
	TupleTableSlot* tupleSlot = node->ss.ss_ScanTupleSlot;

	if(state->ts == NULL) {
		//first call

		ListCell* lc;

		foreach(lc, state->arguments) {
			Argument* argument = lfirst_node(Argument, lc);

			bool isnull = false;

			Datum value = ExecEvalExpr(argument->state,
									   node->ss.ps.ps_ExprContext, &isnull);
			if(isnull) {
				continue;
			}
			switch(argument->type) {
				case F_INT2OUT:
				case F_INT4OUT:
				case F_INT8OUT: {
					argument->value_ref->type = cJSON_Number;
					cJSON_SetIntValue(argument->value_ref,
									  DatumGetInt64(value));
				}
					break;
				case F_FLOAT4OUT: {
					argument->value_ref->type = cJSON_Number;
					cJSON_SetNumberValue(argument->value_ref,
										 (double) DatumGetFloat4(value));
				}
					break;
				case F_FLOAT8OUT: {
					argument->value_ref->type = cJSON_Number;
					cJSON_SetNumberValue(argument->value_ref,
										 DatumGetFloat8(value));
				}
					break;
				case F_VARCHAROUT: {
					argument->value_ref->type = cJSON_Raw;
					argument->value_ref->valuestring = TextDatumGetCString(
							value);
				}
					break;
				case F_ARRAY_OUT: {
					cJSON* array = build_array(value, exprType(
							(Node*) argument->expr));
					argument->value_ref->type = cJSON_Array;
					argument->value_ref->child = array->child;
				}
					break;
				default:
					elog(ERROR, "unsupported value type");
			}
		}

		MemoryContext old_cxt = MemoryContextSwitchTo(
				node->ss.ps.ps_ExprContext->ecxt_per_query_memory);
		state->ts = tuplestore_begin_heap(false, false, work_mem);

		char* debug_request = cJSON_Print(state->query);

		curl_easy_setopt(state->curl, CURLOPT_POSTFIELDS,
						 debug_request);
		CURLcode res = curl_easy_perform(state->curl);
		if(res != CURLE_OK) {
			elog(ERROR, "curl error");
		}
		int http_code;
		curl_easy_getinfo (state->curl, CURLINFO_RESPONSE_CODE, &http_code);

		cJSON* json = cJSON_Parse(state->response);

		if(json == NULL && http_code != 200){
			elog(ERROR, "response code is not 200: %d", http_code);
		}
		
		cJSON* error = NULL;
		cJSON* status = cJSON_GetObjectItem(json, "status");
		//sometimes error is in states object, sometimes it's on top level... 
		if(status == NULL){
			error = cJSON_GetObjectItem(json, "error");
		}else{
			error = cJSON_GetObjectItem(status, "error");
		}

		if(error) {
			elog(ERROR, cJSON_GetStringValue(error));
		}

		cJSON* result = cJSON_GetObjectItem(json, "result");
		cJSON* points = cJSON_GetObjectItem(result, "points");
		if(points == NULL) {
			points = result;
		}
		Datum* values = palloc(
				tupleSlot->tts_tupleDescriptor->natts * sizeof(Datum));
		bool* nulls = palloc(
				tupleSlot->tts_tupleDescriptor->natts * sizeof(bool));

		for(int i = 0; i < cJSON_GetArraySize(points); ++i) {
			cJSON* point = cJSON_GetArrayItem(points, i);
			cJSON* payload = cJSON_GetObjectItem(point, "payload");
			int id = (int) cJSON_GetNumberValue(
					cJSON_GetObjectItem(point, "id"));
			char* payload_str = cJSON_PrintUnformatted(payload);
			char* vector = cJSON_PrintUnformatted(
					cJSON_GetObjectItem(point, "vector"));
			elog(LOG, "id: %d, payload: %s, vector: %s\n", id, payload_str,
				 vector);
			memset(values, 0,
				   tupleSlot->tts_tupleDescriptor->natts * sizeof(Datum));
			memset(nulls, true,
				   tupleSlot->tts_tupleDescriptor->natts * sizeof(bool));

			foreach(lc, state->target_names) {
				QdrantAttribute* attribute = lfirst_node(QdrantAttribute, lc);
				Datum value;
				if(strcmp(attribute->name, "vector") == 0) {
					value = CStringGetTextDatum(vector);
				}else if(strcmp(attribute->name, "id") == 0) {
					value = Int32GetDatum(id);
				}else if(strcmp(attribute->name, "payload") == 0) {
					value = CStringGetTextDatum(payload_str);
				}else {
					cJSON* obj = cJSON_GetObjectItem(payload, attribute->name);
					if(obj == NULL) {
						continue;
					}
					switch(attribute->type) {
						case BOOLOID:
							value = BoolGetDatum(cJSON_IsTrue(obj));
							break;
						case INT2OID:
							value = Int16GetDatum(
									(int16) cJSON_GetNumberValue(obj));
							break;
						case INT4OID:
							value = Int32GetDatum(
									(int32) cJSON_GetNumberValue(obj));
							break;
						case INT8OID:
							value = Int64GetDatum(
									(int64) cJSON_GetNumberValue(obj));
							break;
						case FLOAT4OID:
							value = Float4GetDatum(
									(float4) cJSON_GetNumberValue(obj));
							break;
						case FLOAT8OID:
							value = Float8GetDatum(
									(float8) cJSON_GetNumberValue(obj));
						case TEXTOID:
						case VARCHAROID:
							value = CStringGetTextDatum(
									cJSON_GetStringValue(obj));
							break;
						default:
							elog(ERROR, "unknown type");
					}
				}
				values[attribute->index] = value;
				nulls[attribute->index] = false;
			}

			tuplestore_putvalues(state->ts, tupleSlot->tts_tupleDescriptor,
								 values, nulls);
		}
		cJSON_Delete(json);
		pfree(state->response);
		state->response_size = 0;
		state->pointer = tuplestore_alloc_read_pointer(state->ts, 0);
		MemoryContextSwitchTo(old_cxt);
	}


	ExecClearTuple(tupleSlot);
	tuplestore_select_read_pointer(state->ts, state->pointer);

	bool gotOK = tuplestore_gettupleslot(state->ts, true, false,
										 state->state_slot);

	if(!gotOK) {
		return NULL;
	}

	slot_getallattrs(state->state_slot);

	for(int i = 0; i < state->state_slot->tts_tupleDescriptor->natts; i++) {
		tupleSlot->tts_values[i] = state->state_slot->tts_values[i];
		tupleSlot->tts_isnull[i] = state->state_slot->tts_isnull[i];
	}


	ExecStoreVirtualTuple(tupleSlot);

	return tupleSlot;
}

static void
QdrantReScanForeignScan(ForeignScanState* node) {
	State* state = node->fdw_state;
	if(state->ts == NULL) {
		return;
	}

	if(node->ss.ps.chgParam != NULL) {
		tuplestore_rescan(state->ts);
	}else {
		tuplestore_end(state->ts);
		state->ts = NULL;
	}
}

static void
QdrantEndForeignScan(ForeignScanState* node) {
	State* state = node->fdw_state;
	if(state == NULL) {
		return;
	}
	if(state->ts) {
		tuplestore_end(state->ts);
	}
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
}
