package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

/**
 * The Query Response interface is a generic class that will be implemented by all the responses returned from the
 * endpoints. Since a jsonify approach is used there is no need for any particular structure.
 * This interface is a part of the generic query response procedure that follows the steps below:
 * Endpoint -> QueryTypes -> creates a QueryPlan -> the QueryPlan is executed -> a QueryResponse is returned
 */
public interface QueryResponse {
}
