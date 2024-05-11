#include "filter.h"
#include "utils/syscache.h"
#include "catalog/pg_operator.h"
#include <utils/lsyscache.h>
#include <nodes/nodeFuncs.h>

typedef enum {
	COND_EQ,
	COND_NOT_EQ,
	COND_GT,
	COND_GTE,
	COND_LT,
	COND_LTE
} QdrantCOND;

static inline char* get_op_name(BoolExprType type){
	switch(type) {
		case AND_EXPR:
			return "must";
		case OR_EXPR:
			return "should";
		default:
			elog(ERROR, "unsupported operator1");
	}
}

static inline bool normal_clouse(Expr** left, Expr** right){
	
	if(IsA(*right, Var) || IsA(*right, RelabelType)){
		Expr* tmp = *left;
		*left = *right;
		*right = tmp;
	}
	
	if(!(IsA(*left, Var) || IsA(*left, RelabelType))){
		return false;
	}
	
	if(IsA(*left, RelabelType)){
		RelabelType* relabelType = (RelabelType*) *left;
		*left = relabelType->arg;
		return true;
	}
	return true;
}

static inline bool name_is_id(const char* name){
	return strcmp(name, "id") == 0;
}

static inline bool name_is_vector(const char* name){
	return strcmp(name, "vector") == 0;
}

static inline const char* get_name (TupleDesc desc, Index index){
	return (const char*) &desc->attrs[index - 1].attname;
}

static inline QdrantCOND get_cond (Oid opno){
	bool is_null;
	HeapTuple op_tuple = SearchSysCache1(OPEROID, opno);
	Datum opname_datum = SysCacheGetAttr(OPEROID, op_tuple, Anum_pg_operator_oprname, &is_null);
	QdrantCOND cond;
	if(strcmp("=", DatumGetCString(opname_datum)) == 0){
		cond = COND_EQ;
	}else if(strcmp(">", DatumGetCString(opname_datum)) == 0){
		cond = COND_GT;
	}else if(strcmp(">=", DatumGetCString(opname_datum)) == 0){
		cond = COND_GTE;
	}else if(strcmp("<", DatumGetCString(opname_datum)) == 0){
		cond = COND_LT;
	}else if(strcmp("<=", DatumGetCString(opname_datum)) == 0){
		cond = COND_LTE;
	}else if(strcmp("<>", DatumGetCString(opname_datum)) == 0){
		cond = COND_NOT_EQ;
	}else{
		elog(ERROR, "unsupport operator: %s", DatumGetCString(opname_datum));
	}

	ReleaseSysCache(op_tuple);
	return cond;
}

static inline Argument* get_condition(cJSON* cond, QdrantCOND op, bool is_array){
	Argument* argument = palloc0(sizeof (Argument));
	argument->value_ref = cJSON_CreateNull();
	cJSON* operator = cJSON_CreateObject();
	switch(op) {
		case COND_EQ:
		case COND_NOT_EQ: /* for not equal we will add must_not above */
			if(is_array){
				cJSON_AddItemToObject(operator, "any",argument->value_ref);
			}else{
				cJSON_AddItemToObject(operator, "value",argument->value_ref);
			}
			cJSON_AddItemToObject(cond, "match", operator);
			break;
		case COND_GT:
			cJSON_AddItemToObject(operator, "gt",argument->value_ref);
			cJSON_AddItemToObject(cond, "range", operator);
			break;
		case COND_GTE:
			cJSON_AddItemToObject(operator, "gte",argument->value_ref);
			cJSON_AddItemToObject(cond, "range", operator);
			break;
		case COND_LT:
			cJSON_AddItemToObject(operator, "lt",argument->value_ref);
			cJSON_AddItemToObject(cond, "range", operator);
			break;
		case COND_LTE:
			cJSON_AddItemToObject(operator, "lte",argument->value_ref);
			cJSON_AddItemToObject(cond, "range", operator);
			break;
		default:
			elog(ERROR, "unsupported operator");
	}
	
	return argument;
}

static inline Var* pull_var(Expr* expr){
	if(IsA(expr, Var)){
		return (Var*) expr;
	}
	if(IsA(expr, RelabelType)){
		RelabelType* relabelType = (RelabelType*) expr;
		return (Var*) relabelType->arg;
	}
}

cJSON* build_filter (TupleDesc desc, List** result, Expr* expr, AttributeType* attribute_map){
	bool		isvarlena;

	switch(expr->type) {
		case T_BoolExpr:
		{
			BoolExpr* boolExpr = (BoolExpr*) expr;
			cJSON* filter = cJSON_CreateObject();
			cJSON* op = cJSON_CreateArray();

			cJSON_AddItemToObject(filter, get_op_name(boolExpr->boolop), op);

			ListCell* lc;
			foreach(lc, boolExpr->args){
				cJSON_AddItemToArray(op, build_filter(desc, result, lfirst(lc),
													  attribute_map));
			}
			
			return filter;
		}
		case T_OpExpr:
		{
			OpExpr* opExpr = (OpExpr*) expr;
			Expr* left = list_nth(opExpr->args, 0);
			Expr* right = list_nth(opExpr->args, 1);
			if(!normal_clouse(&left, &right)){
				return NULL;
			}
			Var* var = (Var*) left;
			cJSON* json_expr = cJSON_CreateObject();
			QdrantCOND cond = get_cond(opExpr->opno);
			Argument* argument;
			bool is_id = name_is_id(get_name(desc, var->varattno));
			if(is_id){
				if(cond != COND_EQ){
					elog(ERROR, "id support only = operator");
				}

				argument = palloc0(sizeof(Argument));
				argument->value_ref = cJSON_CreateNull();
				cJSON* array = cJSON_CreateArray();
				cJSON_AddItemToArray(array, argument->value_ref);
				cJSON_AddItemToObject(json_expr, "has_id", array);
				*result = lappend(*result, argument);
			}else{
				if(name_is_vector(get_name(desc, var->varattno))){
					(*attribute_map) |= HAS_VECTOR;
				}else{
					(*attribute_map) |= HAS_PAYLOAD;
				}
				
				cJSON_AddItemToObject(json_expr, "key", cJSON_CreateString(
						get_name(desc, var->varattno)));
				argument = get_condition(json_expr, cond, false);
				
				*result = lappend(*result, argument);
			}
			argument->expr = right;
			

			getTypeOutputInfo(exprType((Node *) right),&argument->type, &isvarlena );
			
			if(cond == COND_NOT_EQ){
				cJSON* must_not = cJSON_CreateObject();
				cJSON_AddItemToObject(must_not, "must_not",
									  cJSON_CreateArrayReference(json_expr));
				return must_not;
			}
			
			return json_expr;
		}
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr* scalarArrayOpExpr = (ScalarArrayOpExpr*) expr;
			Expr* left = list_nth(scalarArrayOpExpr->args, 0);
			Expr* right = list_nth(scalarArrayOpExpr->args, 1);
			if(!normal_clouse(&left, &right)){
				return NULL;
			}
			Var* var = (Var*) left;
			cJSON* json_expr = cJSON_CreateObject();
			cJSON_AddItemToObject(json_expr, "key", cJSON_CreateString(
					get_name(desc, var->varattno)));
			QdrantCOND cond = get_cond(scalarArrayOpExpr->opno);
			if(cond != COND_EQ && cond != COND_NOT_EQ){
				elog(ERROR, "array only support = and <> operators");
			}

			if(name_is_vector(get_name(desc, var->varattno))) {
				(*attribute_map) |= HAS_VECTOR;
			}else if(!name_is_id(get_name(desc, var->varattno))){
				(*attribute_map) |= HAS_PAYLOAD;
			}
			
			Argument* argument = get_condition(json_expr, cond, true);
			argument->expr = right;
			*result = lappend(*result, argument);

			getTypeOutputInfo(exprType((Node*) right),&argument->type, &isvarlena );

			if(cond == COND_NOT_EQ){
				cJSON* must_not = cJSON_CreateObject();
				cJSON_AddItemToObject(must_not, "must_not",
									  cJSON_CreateArrayReference(json_expr));
				return must_not;
			}
			
			return json_expr;
		}
		default:
			return NULL;
	}
	
	return NULL;
}

bool can_pushdown_filter (TupleDesc desc, Expr* expr){
	switch(expr->type) {
		case T_BoolExpr: {
			BoolExpr* boolExpr = (BoolExpr*) expr;

			ListCell* lc;
			foreach(lc, boolExpr->args) {
				if(!can_pushdown_filter(desc, lfirst(lc))){
					return false;
				}
			}

			return true;
		}
		case T_OpExpr: {
			OpExpr* opExpr = (OpExpr*) expr;
			Expr* left = list_nth(opExpr->args, 0);
			Expr* right = list_nth(opExpr->args, 1);
			if(!normal_clouse(&left, &right)){
				return false;
			}
			Var* var = (Var*) left;
			QdrantCOND cond = get_cond(opExpr->opno);
			
			bool is_id = name_is_id(get_name(desc, var->varattno));
			if(is_id && cond != COND_EQ) {
				return false;
			}

			return true;
		}

		case T_ScalarArrayOpExpr:{
			ScalarArrayOpExpr* scalarArrayOpExpr = (ScalarArrayOpExpr*) expr;
			Expr* left = list_nth(scalarArrayOpExpr->args, 0);
			Expr* right = list_nth(scalarArrayOpExpr->args, 1);
			if(!normal_clouse(&left, &right)){
				return NULL;
			}

			QdrantCOND cond = get_cond(scalarArrayOpExpr->opno);
			if(cond != COND_EQ && cond != COND_NOT_EQ){
				return false;
			}
			
			return true;
		}
		
		default:
			return false;
	}
}