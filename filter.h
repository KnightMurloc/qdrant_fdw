#ifndef QDRANT_FDW_FILTER_H
#define QDRANT_FDW_FILTER_H

#include "cJSON/cJSON.h"

#include <postgres.h>

#include <access/tupdesc.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>

typedef struct Argument {
	cJSON* value_ref;
	Expr* expr;
	ExprState* state;
	Oid type;
} Argument;

typedef enum {
	HAS_VECTOR = 1 << 0,
	HAS_PAYLOAD = 1 << 1
}AttributeType;

cJSON* build_filter (TupleDesc desc, List** result, Expr* expr, AttributeType* attribute_map);

bool can_pushdown_filter (TupleDesc desc, Expr* expr);

#endif //QDRANT_FDW_FILTER_H
