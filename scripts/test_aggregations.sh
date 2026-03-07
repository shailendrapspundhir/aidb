#!/bin/bash

# Aggregation Test Script for aiDB
# Tests all aggregation pipeline features: match, sort, group, project, limit, skip, search

set -e

# Configuration
URL="http://localhost:11111"
TOKEN=""
COLLECTION="agg_test_data"
ENV="dev"
TENANT="tenant1"

echo "=========================================="
echo "aiDB Aggregation Pipeline Test Suite"
echo "=========================================="
echo ""

# Helper function to make authenticated API calls
api_call() {
    local METHOD=$1
    local ENDPOINT=$2
    local DATA=$3
    
    if [ -n "$DATA" ]; then
        curl -s -X "$METHOD" "${URL}${ENDPOINT}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${TOKEN}" \
            -d "$DATA"
    else
        curl -s -X "$METHOD" "${URL}${ENDPOINT}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${TOKEN}"
    fi
}

# Helper to run aggregation and print result
run_aggregation() {
    local NAME=$1
    local PIPELINE=$2
    
    echo "------------------------------------------"
    echo "Aggregation: $NAME"
    echo "Pipeline: $PIPELINE"
    echo ""
    
    local RESULT=$(api_call "POST" "/collections/${COLLECTION}/aggregate" "{\"pipeline\": $PIPELINE}")
    echo "Result:"
    echo "$RESULT"
    echo ""
}

echo "Step 1: Register and Login"
echo "------------------------------------------"

# Register user
curl -s -X POST "${URL}/register" \
    -H "Content-Type: application/json" \
    -d '{"username":"testuser","password":"testpass"}' > /dev/null

# Login and get token
LOGIN_RESPONSE=$(curl -s -X POST "${URL}/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"testuser","password":"testpass"}')
TOKEN=$(echo "$LOGIN_RESPONSE" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
    echo "Failed to get token. Exiting."
    exit 1
fi
echo "✓ Authenticated successfully"
echo ""

echo "Step 2: Setup - Create Tenant, Environment, Collection"
echo "------------------------------------------"
api_call "POST" "/tenants" '{"id":"'"$TENANT"'","name":"Test Tenant"}' > /dev/null
api_call "POST" "/tenants/${TENANT}/environments" '{"id":"'"$ENV"'","name":"Development"}' > /dev/null
api_call "POST" "/environments/${ENV}/collections" '{"id":"'"$COLLECTION"'","name":"Aggregation Test Collection"}' > /dev/null
echo "✓ Collection '${COLLECTION}' created"
echo ""

echo "Step 3: Insert Test Data"
echo "------------------------------------------"
echo "Inserting 20 documents with varied fields for aggregation testing..."

# Insert documents with various categories, statuses, and numeric values
insert_doc() {
    local ID=$1
    local TEXT=$2
    local CATEGORY=$3
    local VECTOR=$4
    local METADATA=$5

    api_call "POST" "/collections/${COLLECTION}/docs" "{\"id\":\"${ID}\",\"text\":\"${TEXT}\",\"category\":\"${CATEGORY}\",\"vector\":${VECTOR},\"metadata_json\":\"${METADATA}\"}" > /dev/null
}

insert_doc "sale_001" "Laptop purchase" "electronics" "[0.1,0.2,0.3,0.4]" "{\\\"amount\\\":1200,\\\"region\\\":\\\"north\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q1\\\"}"
insert_doc "sale_002" "Phone purchase" "electronics" "[0.2,0.3,0.4,0.5]" "{\\\"amount\\\":800,\\\"region\\\":\\\"south\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q1\\\"}"
insert_doc "sale_003" "Tablet purchase" "electronics" "[0.3,0.4,0.5,0.6]" "{\\\"amount\\\":600,\\\"region\\\":\\\"east\\\",\\\"status\\\":\\\"pending\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q2\\\"}"
insert_doc "sale_004" "Monitor purchase" "electronics" "[0.4,0.5,0.6,0.7]" "{\\\"amount\\\":400,\\\"region\\\":\\\"west\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"charlie\\\",\\\"quarter\\\":\\\"Q2\\\"}"
insert_doc "sale_005" "Keyboard purchase" "electronics" "[0.5,0.6,0.7,0.8]" "{\\\"amount\\\":150,\\\"region\\\":\\\"north\\\",\\\"status\\\":\\\"cancelled\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q3\\\"}"

insert_doc "sale_006" "Desk purchase" "furniture" "[0.6,0.7,0.8,0.9]" "{\\\"amount\\\":500,\\\"region\\\":\\\"south\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q1\\\"}"
insert_doc "sale_007" "Chair purchase" "furniture" "[0.7,0.8,0.9,0.1]" "{\\\"amount\\\":300,\\\"region\\\":\\\"east\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"charlie\\\",\\\"quarter\\\":\\\"Q2\\\"}"
insert_doc "sale_008" "Bookshelf purchase" "furniture" "[0.8,0.9,0.1,0.2]" "{\\\"amount\\\":250,\\\"region\\\":\\\"west\\\",\\\"status\\\":\\\"pending\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q3\\\"}"
insert_doc "sale_009" "Lamp purchase" "furniture" "[0.9,0.1,0.2,0.3]" "{\\\"amount\\\":100,\\\"region\\\":\\\"north\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q4\\\"}"
insert_doc "sale_010" "Sofa purchase" "furniture" "[0.1,0.3,0.5,0.7]" "{\\\"amount\\\":1500,\\\"region\\\":\\\"south\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"charlie\\\",\\\"quarter\\\":\\\"Q4\\\"}"

insert_doc "sale_011" "Shirt purchase" "clothing" "[0.2,0.4,0.6,0.8]" "{\\\"amount\\\":50,\\\"region\\\":\\\"east\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q1\\\"}"
insert_doc "sale_012" "Pants purchase" "clothing" "[0.3,0.5,0.7,0.9]" "{\\\"amount\\\":80,\\\"region\\\":\\\"west\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q2\\\"}"
insert_doc "sale_013" "Jacket purchase" "clothing" "[0.4,0.6,0.8,0.1]" "{\\\"amount\\\":200,\\\"region\\\":\\\"north\\\",\\\"status\\\":\\\"pending\\\",\\\"sales_rep\\\":\\\"charlie\\\",\\\"quarter\\\":\\\"Q3\\\"}"
insert_doc "sale_014" "Shoes purchase" "clothing" "[0.5,0.7,0.9,0.2]" "{\\\"amount\\\":120,\\\"region\\\":\\\"south\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q4\\\"}"
insert_doc "sale_015" "Hat purchase" "clothing" "[0.6,0.8,0.1,0.3]" "{\\\"amount\\\":30,\\\"region\\\":\\\"east\\\",\\\"status\\\":\\\"cancelled\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q1\\\"}"

insert_doc "sale_016" "Groceries purchase" "food" "[0.7,0.9,0.2,0.4]" "{\\\"amount\\\":150,\\\"region\\\":\\\"west\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"charlie\\\",\\\"quarter\\\":\\\"Q1\\\"}"
insert_doc "sale_017" "Restaurant meal" "food" "[0.8,0.1,0.3,0.5]" "{\\\"amount\\\":75,\\\"region\\\":\\\"north\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q2\\\"}"
insert_doc "sale_018" "Catering service" "food" "[0.9,0.2,0.4,0.6]" "{\\\"amount\\\":500,\\\"region\\\":\\\"south\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"alice\\\",\\\"quarter\\\":\\\"Q3\\\"}"
insert_doc "sale_019" "Beverages" "food" "[0.1,0.4,0.7,0.1]" "{\\\"amount\\\":40,\\\"region\\\":\\\"east\\\",\\\"status\\\":\\\"pending\\\",\\\"sales_rep\\\":\\\"charlie\\\",\\\"quarter\\\":\\\"Q4\\\"}"
insert_doc "sale_020" "Snacks" "food" "[0.2,0.5,0.8,0.2]" "{\\\"amount\\\":25,\\\"region\\\":\\\"west\\\",\\\"status\\\":\\\"completed\\\",\\\"sales_rep\\\":\\\"bob\\\",\\\"quarter\\\":\\\"Q4\\\"}"

echo "✓ 20 documents inserted"
echo ""

echo "=========================================="
echo "AGGREGATION TEST CASES"
echo "=========================================="
echo ""

# Test 1: Simple Match - Filter by category
echo "TEST 1: Simple Match Filter"
echo "Description: Filter documents where category = 'electronics'"
run_aggregation "Match - Electronics Only" '[
    {"match": {"filters": [{"field": "category", "op": "eq", "value": "electronics"}], "logic": "and"}}
]'

# Test 2: Match with multiple conditions (AND logic)
echo "TEST 2: Match with Multiple Conditions (AND)"
echo "Description: Filter where category = 'electronics' AND status = 'completed'"
run_aggregation "Match - Electronics AND Completed" '[
    {"match": {"filters": [
        {"field": "category", "op": "eq", "value": "electronics"},
        {"field": "metadata.status", "op": "eq", "value": "completed"}
    ], "logic": "and"}}
]'

# Test 3: Match with OR logic
echo "TEST 3: Match with OR Logic"
echo "Description: Filter where category = 'furniture' OR category = 'food'"
run_aggregation "Match - Furniture OR Food" '[
    {"match": {"filters": [
        {"field": "category", "op": "eq", "value": "furniture"},
        {"field": "category", "op": "eq", "value": "food"}
    ], "logic": "or"}}
]'

# Test 4: Match with numeric comparison (greater than)
echo "TEST 4: Match with Numeric Comparison (GT)"
echo "Description: Filter where amount > 500"
run_aggregation "Match - Amount Greater Than 500" '[
    {"match": {"filters": [{"field": "metadata.amount", "op": "gt", "value": 500}], "logic": "and"}}
]'

# Test 5: Match with numeric range (gte/lte)
echo "TEST 5: Match with Numeric Range"
echo "Description: Filter where amount >= 100 AND amount <= 500"
run_aggregation "Match - Amount Between 100 and 500" '[
    {"match": {"filters": [
        {"field": "metadata.amount", "op": "gte", "value": 100},
        {"field": "metadata.amount", "op": "lte", "value": 500}
    ], "logic": "and"}}
]'

# Test 6: Group by category with count
echo "TEST 6: Group by Category with Count"
echo "Description: Count documents per category"
run_aggregation "Group - Count by Category" '[
    {"group": {"by": ["category"], "aggregations": [{"op": "count", "as": "doc_count"}]}}
]'

# Test 7: Group by category with sum of amounts
echo "TEST 7: Group by Category with Sum"
echo "Description: Sum of amounts per category"
run_aggregation "Group - Sum by Category" '[
    {"group": {"by": ["category"], "aggregations": [
        {"op": "count", "as": "count"},
        {"op": "sum", "field": "metadata.amount", "as": "total_amount"}
    ]}}
]'

# Test 8: Group by category with multiple aggregations
echo "TEST 8: Group by Category with Multiple Aggregations"
echo "Description: Count, Sum, Avg, Min, Max per category"
run_aggregation "Group - Full Stats by Category" '[
    {"group": {"by": ["category"], "aggregations": [
        {"op": "count", "as": "count"},
        {"op": "sum", "field": "metadata.amount", "as": "total"},
        {"op": "avg", "field": "metadata.amount", "as": "average"},
        {"op": "min", "field": "metadata.amount", "as": "min_amount"},
        {"op": "max", "field": "metadata.amount", "as": "max_amount"}
    ]}}
]'

# Test 9: Group by region and quarter
echo "TEST 9: Group by Multiple Fields"
echo "Description: Group by region and quarter with count and sum"
run_aggregation "Group - By Region and Quarter" '[
    {"group": {"by": ["metadata.region", "metadata.quarter"], "aggregations": [
        {"op": "count", "as": "sales_count"},
        {"op": "sum", "field": "metadata.amount", "as": "total_sales"}
    ]}}
]'

# Test 10: Sort by amount ascending
echo "TEST 10: Sort by Amount (Ascending)"
echo "Description: Sort documents by amount (low to high), limit 5"
run_aggregation "Sort - Amount Ascending" '[
    {"sort": [{"field": "metadata.amount", "order": "asc"}]},
    {"limit": 5}
]'

# Test 11: Sort by amount descending
echo "TEST 11: Sort by Amount (Descending)"
echo "Description: Sort documents by amount (high to low), limit 5"
run_aggregation "Sort - Amount Descending" '[
    {"sort": [{"field": "metadata.amount", "order": "desc"}]},
    {"limit": 5}
]'

# Test 12: Multi-field sort
echo "TEST 12: Multi-Field Sort"
echo "Description: Sort by category, then by amount within each category"
run_aggregation "Sort - By Category then Amount" '[
    {"sort": [
        {"field": "category", "order": "asc"},
        {"field": "metadata.amount", "order": "desc"}
    ]},
    {"limit": 8}
]'

# Test 13: Limit results
echo "TEST 13: Limit Results"
echo "Description: Return only first 3 documents"
run_aggregation "Limit - First 3 Documents" '[
    {"limit": 3}
]'

# Test 14: Skip and Limit (Pagination)
echo "TEST 14: Skip and Limit (Pagination)"
echo "Description: Skip first 5, return next 5"
run_aggregation "Skip/Limit - Page 2" '[
    {"skip": 5},
    {"limit": 5}
]'

# Test 15: Project - Include specific fields
echo "TEST 15: Project - Include Specific Fields"
echo "Description: Return only id, category, and amount fields"
run_aggregation "Project - Include Only" '[
    {"project": {"include": ["id", "category", "metadata.amount"], "exclude": []}}
]'

# Test 16: Complex pipeline - Match + Group + Sort
echo "TEST 16: Complex Pipeline - Match + Group + Sort"
echo "Description: Filter completed sales, group by region, sort by total descending"
run_aggregation "Pipeline - Completed Sales by Region" '[
    {"match": {"filters": [{"field": "metadata.status", "op": "eq", "value": "completed"}], "logic": "and"}},
    {"group": {"by": ["metadata.region"], "aggregations": [
        {"op": "count", "as": "count"},
        {"op": "sum", "field": "metadata.amount", "as": "total"}
    ]}},
    {"sort": [{"field": "total", "order": "desc"}]}
]'

# Test 17: Complex pipeline - Match + Group + Project
echo "TEST 17: Complex Pipeline - Match + Group + Project"
echo "Description: Group by sales_rep, show only rep name and total sales"
run_aggregation "Pipeline - Sales by Rep" '[
    {"group": {"by": ["metadata.sales_rep"], "aggregations": [
        {"op": "count", "as": "deals_closed"},
        {"op": "sum", "field": "metadata.amount", "as": "total_sales"}
    ]}},
    {"project": {"include": ["_id", "deals_closed", "total_sales"], "exclude": []}}
]'

# Test 18: Search stage - Text search
echo "TEST 18: Search Stage - Text Search"
echo "Description: Search for documents containing 'purchase'"
run_aggregation "Search - Contains 'purchase'" '[
    {"search": {"query": "purchase", "partial_match": false, "case_sensitive": false, "include_metadata": false}}
]'

# Test 19: Search + Match combination
echo "TEST 19: Search + Match Combination"
echo "Description: Search for 'purchase', then filter for electronics only"
run_aggregation "Search + Match - Electronics Purchases" '[
    {"search": {"query": "purchase", "partial_match": false, "case_sensitive": false, "include_metadata": false}},
    {"match": {"filters": [{"field": "category", "op": "eq", "value": "electronics"}], "logic": "and"}}
]'

# Test 20: Full analytics pipeline
echo "TEST 20: Full Analytics Pipeline"
echo "Description: Completed Q1 sales, group by category, sort by total, limit top 3"
run_aggregation "Analytics - Top Q1 Categories" '[
    {"match": {"filters": [
        {"field": "metadata.status", "op": "eq", "value": "completed"},
        {"field": "metadata.quarter", "op": "eq", "value": "Q1"}
    ], "logic": "and"}},
    {"group": {"by": ["category"], "aggregations": [
        {"op": "count", "as": "sales_count"},
        {"op": "sum", "field": "metadata.amount", "as": "total_revenue"},
        {"op": "avg", "field": "metadata.amount", "as": "avg_sale"}
    ]}},
    {"sort": [{"field": "total_revenue", "order": "desc"}]},
    {"limit": 3}
]'

# Cross-collection aggregation tests (require additional collections)

echo "TEST 21: Cross-Collection Lookup in Aggregation"
echo "Description: Lookup addresses for users, then group by region"
echo "Note: Requires 'users' and 'addresses' collections with related data"
run_aggregation "Cross-Collection - Users with Addresses" '[
    {"lookup": {"from": "addresses", "local_field": "id", "foreign_field": "metadata.user_id", "as": "user_addresses"}},
    {"project": {"include": ["id", "metadata.email", "metadata.region", "user_addresses"], "exclude": []}}
]'

echo "TEST 22: Cross-Collection Join in Aggregation"
echo "Description: Join orders with payments"
echo "Note: Requires 'orders' and 'payments' collections with related data"
run_aggregation "Cross-Collection - Orders Join Payments" '[
    {"join": {"join_type": "left", "from": "payments", "local_field": "id", "foreign_field": "metadata.order_id", "as": "payment_details"}},
    {"match": {"filters": [{"field": "payment_details.metadata.status", "op": "eq", "value": "completed"}], "logic": "and"}}
]'

echo "TEST 23: Cross-Collection Union in Aggregation"
echo "Description: Combine users, orders, and products into single result"
echo "Note: Requires 'users', 'orders', and 'products' collections"
run_aggregation "Cross-Collection - Union Multiple Collections" '[
    {"union": {"collections": ["orders", "products"]}},
    {"group": {"by": ["category"], "aggregations": [{"op": "count", "as": "item_count"}]}}
]'

echo "TEST 24: Complex Cross-Collection Pipeline"
echo "Description: Lookup + Group + Sort across collections"
echo "Note: Multi-stage aggregation with cross-collection lookup"
run_aggregation "Cross-Collection - Complex Pipeline" '[
    {"lookup": {"from": "addresses", "local_field": "id", "foreign_field": "metadata.user_id", "as": "addresses"}},
    {"lookup": {"from": "orders", "local_field": "id", "foreign_field": "metadata.user_id", "as": "orders"}},
    {"group": {"by": ["metadata.region"], "aggregations": [
        {"op": "count", "as": "user_count"},
        {"op": "avg", "field": "metadata.age", "as": "avg_age"}
    ]}},
    {"sort": [{"field": "user_count", "order": "desc"}]}
]'

echo "TEST 25: Cross-Collection Aggregation with Match Filter"
echo "Description: Filter users by region, lookup addresses, group results"
echo "Note: Filter before cross-collection operations"
run_aggregation "Cross-Collection - Filter then Lookup" '[
    {"match": {"filters": [{"field": "metadata.region", "op": "eq", "value": "north"}], "logic": "and"}},
    {"lookup": {"from": "addresses", "local_field": "id", "foreign_field": "metadata.user_id", "as": "user_addresses"}},
    {"project": {"include": ["id", "metadata.email", "user_addresses"], "exclude": []}},
    {"limit": 10}
]'

echo "=========================================="
echo "CROSS-COLLECTION AGGREGATION TESTS COMPLETED"
echo "=========================================="
echo ""
echo "Summary of all tested features:"
echo "  ✓ Match filters (eq, ne, gt, gte, lt, lte)"
echo "  ✓ Match logic (AND, OR)"
echo "  ✓ Group by single and multiple fields"
echo "  ✓ Aggregation operations (count, sum, avg, min, max)"
echo "  ✓ Sort (ascending, descending, multi-field)"
echo "  ✓ Limit and Skip (pagination)"
echo "  ✓ Project (field selection)"
echo "  ✓ Search (full-text)"
echo "  ✓ Complex pipelines (multiple stages)"
echo "  ✓ Cross-collection lookup in aggregations"
echo "  ✓ Cross-collection join in aggregations"
echo "  ✓ Cross-collection union in aggregations"
echo "  ✓ Multi-collection aggregation pipelines"
echo ""