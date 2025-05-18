import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from neo4j import GraphDatabase
import plotly.express as px
import plotly.graph_objects as go

# Set page config
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="🛒",
    layout="wide"
)

# Initialize session state variables if they don't exist
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Home"
if 'category' not in st.session_state:
    st.session_state.category = None
if 'section' not in st.session_state:
    st.session_state.section = None
if 'responses' not in st.session_state:
    st.session_state.responses = {}
if 'neo4j_connected' not in st.session_state:
    st.session_state.neo4j_connected = False
if 'neo4j_driver' not in st.session_state:
    st.session_state.neo4j_driver = None

# Neo4j Connection Function
def connect_to_neo4j(uri, username, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        # Verify connection
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        st.session_state.neo4j_driver = driver
        st.session_state.neo4j_connected = True
        return True
    except Exception as e:
        st.error(f"Failed to connect to Neo4j: {str(e)}")
        st.session_state.neo4j_connected = False
        return False

# Execute Cypher query and return results as DataFrame
def run_neo4j_query(query, params=None):
    if not st.session_state.neo4j_connected or not st.session_state.neo4j_driver:
        st.error("Not connected to Neo4j. Please connect first.")
        return None
    
    try:
        with st.session_state.neo4j_driver.session() as session:
            result = session.run(query, params or {})
            # Convert to DataFrame
            data = [record.data() for record in result]
            if not data:
                return pd.DataFrame()
            return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return None

# Navigation functions
def navigate_to_home():
    st.session_state.current_page = "Home"
    st.session_state.category = None
    st.session_state.section = None

def navigate_to_category(category):
    st.session_state.current_page = "Category"
    st.session_state.category = category
    st.session_state.section = None

def navigate_to_section(section):
    st.session_state.section = section

# Define questions by category and section with corresponding Cypher queries
questions = {
    "Customer Lifecycle & Behavior": {
        "Customer Journey Evolution": [
            {
                "question": "How does the average cart size evolve over a customer's lifetime (by order number)?",
                "query": """
                // Step 0: Match all orders (even empty ones)
                MATCH (u:User)-[:ORDERED]->(o:Order)
                OPTIONAL MATCH (o)-[:CONTAINS]->(p:Product)
                WITH u.user_id AS userId, o.order_number AS orderNum, count(p) AS cartSize
                ORDER BY userId, orderNum

                // Step 1: Collect and split orders correctly
                WITH userId, collect({orderNum: orderNum, cartSize: cartSize}) AS orders
                WITH userId, size(orders) AS totalOrders,
                    CASE 
                    WHEN size(orders) % 2 = 0 THEN size(orders) / 2  // even → exact half
                    ELSE floor(size(orders) / 2)                     // odd → middle in second half
                    END AS splitIndex,
                    orders

                WITH userId, totalOrders,
                    orders[..splitIndex] AS firstHalf,
                    orders[splitIndex..] AS secondHalf,
                    orders AS allOrders

                // Step 2: Compute average cart sizes
                WITH userId, totalOrders,
                    CASE WHEN size(firstHalf) > 0 THEN reduce(s = 0.0, o IN firstHalf | s + o.cartSize) / size(firstHalf) ELSE 0.0 END AS avgCartSizeFirstHalf,
                    CASE WHEN size(secondHalf) > 0 THEN reduce(s = 0.0, o IN secondHalf | s + o.cartSize) / size(secondHalf) ELSE 0.0 END AS avgCartSizeSecondHalf,
                    CASE WHEN size(allOrders) > 0 THEN reduce(s = 0.0, o IN allOrders | s + o.cartSize) / size(allOrders) ELSE 0.0 END AS avgCartSizeAll

                // Step 3: % change and behavior tag
                WITH userId, totalOrders,
                    round(avgCartSizeFirstHalf, 2) AS avgFirst,
                    round(avgCartSizeSecondHalf, 2) AS avgSecond,
                    round(avgCartSizeAll, 2) AS avgAll,
                    CASE 
                    WHEN avgCartSizeFirstHalf <> 0 
                    THEN round(((avgCartSizeSecondHalf - avgCartSizeFirstHalf) / avgCartSizeFirstHalf) * 100, 2)
                    ELSE 0 
                    END AS percentChange

                RETURN userId, totalOrders,
                    avgFirst AS AvgCartSize_FirstHalf,
                    avgSecond AS AvgCartSize_SecondHalf,
                    avgAll AS AvgCartSize_All,
                    percentChange,
                    CASE 
                        WHEN percentChange > 5 THEN "GROWING"
                        WHEN percentChange < -5 THEN "DECLINING"
                        ELSE "STABLE"
                    END AS CartBehavior
                ORDER BY percentChange DESC;

                """
            },
            {
                "question": "What's the typical progression of departments that new customers explore over their first 5 orders?",
                "query": """
                MATCH (u:User)-[:ORDERED]->(o:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WHERE o.order_number <= 5
                WITH o.order_number AS orderNum, d.department AS departmentName, count(*) AS freq
                RETURN orderNum, departmentName, freq
                ORDER BY orderNum, freq DESC;
                """
            },
            {
                "question": "How many aisles do new clients usually browse throughout their first 5 orders?",
                "query": """
                MATCH (:User)-[:ORDERED]->(o:Order)-[:CONTAINS]->(p:Product)-[:IN_AISLE]->(a:Aisle)
                WHERE o.order_number <= 5
                RETURN a.aisle AS aisleName, count(*) AS frequency
                ORDER BY frequency DESC;
                """
            },
            {
                "question": "How does order hour preference evolve for departments over time?",
                "query": """
                MATCH (:User)-[:ORDERED]->(o:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WHERE o.order_hour_of_day IS NOT NULL
                WITH o.order_hour_of_day AS hour, d.department AS department, count(*) AS freq
                RETURN hour, department, freq
                ORDER BY hour, freq DESC;
                """
            },
            {
                "question": "How does customer/basket share vary across product categories over time?",
                "query": """
                // Match orders and link to departments via products
                MATCH (o:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WHERE o.order_dow IS NOT NULL

                // Count products by department per day
                WITH o.order_dow AS dayOfWeek, d.department AS department, count(*) AS deptCount

                // Calculate total items ordered that day
                WITH dayOfWeek, department, deptCount
                WITH dayOfWeek, collect({dept: department, count: deptCount}) AS deptStats

                UNWIND deptStats AS entry
                WITH dayOfWeek, entry.dept AS department, entry.count AS deptCount,
                    reduce(total = 0, d IN deptStats | total + d.count) AS totalCount

                // Calculate share
                RETURN dayOfWeek, department, round(toFloat(deptCount) / totalCount * 100, 2) AS basketSharePct
                ORDER BY dayOfWeek, basketSharePct DESC
                """
            }
        ],
        "Customer Retention & Segmentation": [
            {
                "question": "How does customer retention vary over time based on their preferred shopping department?",
                "query": """
                // Step 1: Determine each user's preferred department
                MATCH (u:User)-[:ORDERED]->(:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WITH u.user_id AS userId, d.department AS dept, count(*) AS freq
                WITH userId, collect({dept: dept, freq: freq}) AS deptFreqs
                UNWIND deptFreqs AS df
                WITH userId, df.dept AS dept, df.freq AS freq
                ORDER BY userId, freq DESC
                WITH userId, collect(dept)[0] AS favoriteDept

                // Step 2: Track retention by order number
                MATCH (u:User {user_id: userId})-[:ORDERED]->(o:Order)
                WITH favoriteDept AS departmentSegment, o.order_number AS orderNum, u.user_id AS uid
                WITH departmentSegment, orderNum, count(DISTINCT uid) AS customers

                // Step 3: Remove duplicate [orderNum, customers] for each departmentSegment
                WITH departmentSegment, customers, min(orderNum) AS orderNum
                RETURN departmentSegment, orderNum, customers
                ORDER BY departmentSegment, orderNum
                """
            },
            {
                "question": "Which products play a central role in the early purchasing patterns of retained customers?",
                "query": """
                // Step 1: Get early products for retained users (5+ orders)
                MATCH (u:User)-[:ORDERED]->(o:Order)
                WITH u, count(o) AS totalOrders
                WHERE totalOrders >= 5
                MATCH (u)-[:ORDERED]->(o:Order)-[:CONTAINS]->(p:Product)
                WHERE o.order_number <= 3
                WITH collect(DISTINCT id(p)) AS seedProducts

                // Step 2: Run personalized PageRank
                CALL gds.pageRank.stream('ProductCoPurchase', {
                    sourceNodes: seedProducts,
                    maxIterations: 50,
                    dampingFactor: 0.85
                })
                YIELD nodeId, score
                WITH gds.util.asNode(nodeId).name AS productName, round(score, 4) AS pageRankScore
                RETURN productName, pageRankScore
                ORDER BY pageRankScore DESC
                LIMIT 20
                """
            },
            {
                "question": "Can we identify distinct customer segments based on ordering day/time patterns?",
                "query": """
                MATCH (u:User)-[:ORDERED]->(o:Order)
                WHERE o.order_hour_of_day IS NOT NULL
                WITH u.user_id AS userId, avg(o.order_hour_of_day) AS avgHour
                WITH userId,
                    CASE 
                        WHEN avgHour < 6 THEN 'Overnight'
                        WHEN avgHour < 12 THEN 'Morning'
                        WHEN avgHour < 18 THEN 'Afternoon'
                        ELSE 'Evening'
                    END AS timeSegment
                RETURN timeSegment, count(*) AS userCount
                ORDER BY userCount DESC
                """
            },
        ],
        "Purchase Pattern Transitions": [
            {
                "question": "Which products are most frequently transitioned into by loyal customers?",
                "query": """
                // Step 1: Find users who have placed more than 5 orders
                MATCH (u:User)-[:ORDERED]->(o:Order)
                WITH u, count(o) AS totalOrders
                WHERE totalOrders > 5

                // Step 2: For each product the user ordered, count how many of their orders it appeared in
                MATCH (u)-[:ORDERED]->(o2:Order)-[:CONTAINS]->(p:Product)
                WITH u.user_id AS userId, p.name AS productName, count(DISTINCT o2) AS productOrderCount, totalOrders
                WHERE toFloat(productOrderCount) / totalOrders >= 0.8

                // Step 3: Aggregate the count of such users per product
                RETURN 
                productName, 
                count(DISTINCT userId) AS retainedUsers
                ORDER BY retainedUsers DESC
                LIMIT 20
                """
            },
            {
                "question": "Which products serve as 'gateway purchases' that lead to exploration of new departments?",
                "query": """
                // Step 1: Identify each user's first order and departments in it
                MATCH (u:User)-[:ORDERED]->(firstOrder:Order)-[:CONTAINS]->(firstProd:Product)-[:IN_DEPARTMENT]->(firstDept:Department)
                WHERE firstOrder.order_number = 1
                WITH u.user_id AS userId, firstProd.name AS gatewayProduct, collect(DISTINCT firstDept.department) AS initialDepts

                // Step 2: Find all later products from new departments
                MATCH (u:User {user_id: userId})-[:ORDERED]->(laterOrder:Order)-[:CONTAINS]->(laterProd:Product)-[:IN_DEPARTMENT]->(laterDept:Department)
                WHERE laterOrder.order_number > 1 AND NOT laterDept.department IN initialDepts

                // Step 3: Count how many new departments were reached via the gateway product
                RETURN 
                gatewayProduct,
                count(DISTINCT laterDept.department) AS newDeptCount
                ORDER BY newDeptCount DESC
                LIMIT 50
                """
            }
        ]
    },
    "Reorder Behavior & Loyalty Drivers": {
        "Reorder Dynamics": [
            {
                "question": "Are customers reordering items popular with others in their first order?",
                "query": """
                // Step 1: Calculate global popularity of each product
                MATCH (p:Product)
                OPTIONAL MATCH (:Order)-[:CONTAINS]->(p)
                WITH p, count(*) AS totalOrders

                // Step 2: Calculate how often it's included in users' first order
                OPTIONAL MATCH (:User)-[:ORDERED]->(o:Order {order_number: 1})-[:CONTAINS]->(p)
                WITH p.name AS productName, totalOrders, count(o) AS firstOrderCount
                WHERE totalOrders > 10
                RETURN 
                productName, 
                totalOrders, 
                firstOrderCount, 
                round(toFloat(firstOrderCount) / totalOrders * 100, 2) AS percentInFirstOrders
                ORDER BY percentInFirstOrders DESC
                LIMIT 20
                """
            },
            {
                "question": "How does reorder frequency vary with days_since_prior_order for frequently purchased items?",
                "query": """
                // Step 1: Get products with high reorder volume
                MATCH (:Order)-[r:CONTAINS]->(p:Product)
                WHERE r.reordered = 1
                WITH p, count(*) AS reorderCount
                WHERE reorderCount > 20

                // Step 2: Analyze reorder gap
                MATCH (o:Order)-[r:CONTAINS]->(p)
                WHERE r.reordered = 1 AND o.days_since_prior_order IS NOT NULL
                WITH p.name AS productName, avg(o.days_since_prior_order) AS avgGap, count(*) AS totalReorders
                RETURN productName, totalReorders, round(avgGap, 2) AS avgDaysBetweenReorders
                ORDER BY avgDaysBetweenReorders ASC
                LIMIT 20
                """
            },
            {
                "question": "What's the relationship between a product's department and reorder likelihood?",
                "query": """
                MATCH (:Order)-[r:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WHERE r.reordered IS NOT NULL
                WITH d.department AS department,
                    count(*) AS totalOrders,
                    count(CASE WHEN r.reordered = 1 THEN 1 END) AS reorderCount
                WHERE totalOrders > 20
                RETURN 
                department,
                totalOrders,
                reorderCount,
                round(toFloat(reorderCount) / totalOrders * 100, 2) AS reorderRatePct
                ORDER BY reorderRatePct DESC
                LIMIT 21
                """
            }
        ],
        "Promotions & Reorder Uplift": [
            {
                "question": "Which products are most often added to cart in the first 3 positions?",
                "query": """
                MATCH (:Order)-[r:CONTAINS]->(p:Product)
                WHERE r.add_to_cart_order IN [1, 2, 3]
                WITH p.name AS productName, count(*) AS addCount
                RETURN productName, addCount
                ORDER BY addCount DESC
                LIMIT 20
                """
            },
            {
                "question": "Do users prefer reordering familiar products early or exploring new ones in the first 3 cart additions?",
                "query": """
                MATCH (:Order)-[r:CONTAINS]->(p:Product)
                WHERE r.add_to_cart_order IN [1, 2, 3]
                WITH 
                CASE WHEN r.reordered = 1 THEN 'Reordered' ELSE 'New' END AS itemType,
                count(*) AS countItems
                RETURN itemType, countItems
                ORDER BY countItems DESC
                """
            },
            {
                "question": "Which product categories have the highest uplift in sales?",
                "query": """
                // Step 1: Get total orders per user
                MATCH (u:User)-[:ORDERED]->(o:Order)
                WITH u.user_id AS userId, count(o) AS totalOrders
                WITH userId, totalOrders, 
                    CASE 
                        WHEN totalOrders % 2 = 0 THEN totalOrders / 2 
                        ELSE toInteger(floor(totalOrders / 2.0)) 
                    END AS earlyLimit

                // Step 2: For each user-product pair, count early vs late purchases
                MATCH (u:User)-[:ORDERED]->(o:Order)-[:CONTAINS]->(p:Product)
                WITH u.user_id AS userId, o.order_number AS orderNum, p.name AS product, earlyLimit
                WITH userId, product, earlyLimit,
                    count(CASE WHEN orderNum <= earlyLimit THEN 1 END) AS earlyPurchases,
                    count(CASE WHEN orderNum > earlyLimit THEN 1 END) AS latePurchases

                // Step 3: Compute per-user uplift and aggregate
                WITH product,
                    round(avg(toFloat(latePurchases - earlyPurchases) / 
                        CASE WHEN earlyPurchases = 0 THEN 1 ELSE earlyPurchases END) * 100, 2) AS avgUpliftPct,
                    sum(earlyPurchases) AS totalEarlyPurchases,
                    sum(latePurchases) AS totalLatePurchases

                RETURN product, totalEarlyPurchases, totalLatePurchases, avgUpliftPct
                ORDER BY avgUpliftPct DESC
                LIMIT 20
                """
            }
        ]
    },
    "Product Affinity, Graphs & Recommendations": {
        "Co-Purchase Patterns & Product Graphs": [
            {
                "question": "What are the top product pairings based on co-purchase frequency?",
                "query": """
                MATCH (p1:Product)-[r:BOUGHTWITH]->(p2:Product)
                WHERE id(p1) < id(p2)  // Avoid duplicate pairs
                RETURN 
                p1.name AS productA, 
                p2.name AS productB, 
                r.weight AS timesBoughtTogether
                ORDER BY timesBoughtTogether DESC
                LIMIT 20
                """
            },
            {
                "question": "What products have the highest cross-department purchase correlation?",
                "query": """
                CALL gds.nodeSimilarity.stream('ProductCoPurchase')
                YIELD node1, node2, similarity
                WITH gds.util.asNode(node1) AS p1, gds.util.asNode(node2) AS p2, similarity
                WHERE similarity > 0.85 AND p1.product_id <> p2.product_id
                MATCH (p1)-[:IN_DEPARTMENT]->(d1:Department), (p2)-[:IN_DEPARTMENT]->(d2:Department)
                WHERE d1.department <> d2.department
                RETURN 
                p1.name AS productA, d1.department AS deptA,
                p2.name AS productB, d2.department AS deptB,
                round(similarity, 3) AS similarityScore
                ORDER BY similarityScore 
                LIMIT 50
                """
            },
            {
                "question": "Which items act as a 'bridge' between different product communities?",
                "query": """
                // Step 1: Find products with both betweenness and community assigned
                MATCH (p:Product)
                WHERE p.betweenness IS NOT NULL AND p.community_louvain IS NOT NULL

                // Step 2: Check if product has neighbors from multiple communities
                MATCH (p)-[:BOUGHTWITH]-(n:Product)
                WHERE n.community_louvain IS NOT NULL AND n.community_louvain <> p.community_louvain

                // Step 3: Return products that connect to different communities
                WITH DISTINCT p, p.community_louvain AS sourceCommunity, p.betweenness AS score

                RETURN 
                p.product_id AS productId,
                p.name AS productName,
                sourceCommunity,
                round(score, 2) AS betweennessScore
                ORDER BY betweennessScore DESC
                LIMIT 20
                """
            }
        ],
        "Community Detection & Recommendation Modeling": [
            {
                "question": "Do high-centrality products (high betweenness) make better recommendations than low-centrality ones?",
                "query": """
                // Categorize by centrality and count purchases
                MATCH (p:Product)
                WHERE p.betweenness IS NOT NULL
                WITH p,
                    CASE 
                        WHEN p.betweenness >= 1000 THEN 'High Centrality'
                        ELSE 'Low Centrality'
                    END AS centralityGroup
                MATCH (:Order)-[:CONTAINS]->(p)
                RETURN centralityGroup, count(*) AS totalOrders
                ORDER BY totalOrders DESC
                """
            }
        ]
    },
    "Demand Trends & Seasonality": {
        "Product Lifecycle & Decline": [
            {
                "question": "Which products show the sharpest drop in demand over time?",
                "query": """
                // Step 1: For each user, find total orders
                MATCH (u:User)-[:ORDERED]->(o:Order)
                WITH u.user_id AS userId, count(o) AS totalOrders

                // Step 2: Attach each order's products along with order number
                MATCH (u:User {user_id: userId})-[:ORDERED]->(o:Order)-[:CONTAINS]->(p:Product)
                WITH userId, p.name AS productName, o.order_number AS orderNum, totalOrders

                // Step 3: Define first half and second half dynamically
                WITH userId, productName, orderNum, totalOrders,
                    CASE 
                    WHEN totalOrders % 2 = 0 THEN totalOrders / 2   // even split
                    ELSE (totalOrders - 1) / 2                      // odd split: lesser half
                    END AS firstHalfSize

                // Step 4: Classify purchases into firstHalf or secondHalf
                WITH userId, productName,
                    CASE WHEN orderNum <= firstHalfSize THEN 'firstHalf' ELSE 'secondHalf' END AS half

                // Step 5: Aggregate for each product whether it was bought in first or second half
                WITH productName, collect(DISTINCT half) AS halves

                // Step 6: Only keep products bought ONLY in first half (never in second)
                WHERE NOT 'secondHalf' IN halves

                // Step 7: Count how many customers showed this behavior for each product
                RETURN productName
                """
            }
        ],
        "Seasonal Effects": [
            {
                "question": "Which product categories exhibit strong day-of-week effects?",
                "query": """
                MATCH (o:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WHERE o.order_dow IS NOT NULL
                WITH d.department AS department, o.order_dow AS dayOfWeek, COUNT(*) AS orderCount
                WITH department, collect(orderCount) AS weeklyPattern,
                    min(orderCount) AS minCount, max(orderCount) AS maxCount
                WITH department, weeklyPattern,
                    round((toFloat(maxCount - minCount) / maxCount) * 100, 2) AS volatilityPct
                RETURN department, weeklyPattern, volatilityPct
                ORDER BY volatilityPct DESC
                LIMIT 20
                """
            },
            {
                "question": "What is the day-of-week effect on order size across different product categories?",
                "query": """
                // Step 1: Link orders to products and departments
                MATCH (o:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WHERE o.order_dow IS NOT NULL

                // Step 2: Count number of products per (order, department, day)
                WITH o.order_id AS orderId, o.order_dow AS dayOfWeek, d.department AS department, count(p) AS productCount

                // Step 3: Get average cart size (order size) per department per day
                WITH department, dayOfWeek, avg(productCount) AS avgCartSize

                // Step 4: Collect all day values into a list for each department
                WITH department, collect({day: dayOfWeek, cartSize: avgCartSize}) AS cartStats

                // Step 5: Determine highest and lowest days per department
                WITH department,
                    reduce(maxDay = cartStats[0], entry IN cartStats |
                        CASE WHEN entry.cartSize > maxDay.cartSize THEN entry ELSE maxDay END) AS highest,
                    reduce(minDay = cartStats[0], entry IN cartStats |
                        CASE WHEN entry.cartSize < minDay.cartSize THEN entry ELSE minDay END) AS lowest

                RETURN department,
                    CASE highest.day
                        WHEN 0 THEN "Sunday"
                        WHEN 1 THEN "Monday"
                        WHEN 2 THEN "Tuesday"
                        WHEN 3 THEN "Wednesday"
                        WHEN 4 THEN "Thursday"
                        WHEN 5 THEN "Friday"
                        WHEN 6 THEN "Saturday"
                    END AS highestCartDay,
                    round(highest.cartSize, 2) AS highestAvgCartSize,
                    CASE lowest.day
                        WHEN 0 THEN "Sunday"
                        WHEN 1 THEN "Monday"
                        WHEN 2 THEN "Tuesday"
                        WHEN 3 THEN "Wednesday"
                        WHEN 4 THEN "Thursday"
                        WHEN 5 THEN "Friday"
                        WHEN 6 THEN "Saturday"
                    END AS lowestCartDay,
                    round(lowest.cartSize, 2) AS lowestAvgCartSize
                ORDER BY department
                """
            }
        ]
    },
    "Supply Chain & Delivery Performance": {
        "Delivery Success Factors": [
            {
                "question": "Which suppliers have the highest delivery success rates?",
                "query": """
                // Step 1: Parse dates properly
                MATCH (s:Supplier)-[:SENDS]->(sh:Shipment)
                WHERE sh.expected_delivery_date IS NOT NULL AND sh.actual_delivery_date IS NOT NULL

                WITH 
                s, sh,
                split(sh.expected_delivery_date, "/") AS expectedParts,
                split(sh.actual_delivery_date, "/") AS actualParts

                WITH 
                s,
                date({year: toInteger('20' + expectedParts[2]), month: toInteger(expectedParts[1]), day: toInteger(expectedParts[0])}) AS expectedDate,
                date({year: toInteger('20' + actualParts[2]), month: toInteger(actualParts[1]), day: toInteger(actualParts[0])}) AS actualDate

                WITH 
                s.supplier_name AS supplier,
                CASE WHEN actualDate <= expectedDate THEN 1 ELSE 0 END AS onTimeFlag

                // Step 2: Aggregate and filter only perfect suppliers
                WITH supplier,
                    sum(onTimeFlag) AS onTimeDeliveries,
                    count(*) AS totalDeliveries
                WHERE onTimeDeliveries = totalDeliveries

                RETURN supplier
                ORDER BY supplier
                """
            },
            {
                "question": "What proportion of shipments are in transit, delivered and dispatched?",
                "query": """
                // Step 1: Match all shipments that have a status
                MATCH (sh:Shipment)
                WHERE sh.status IS NOT NULL

                // Step 2: Group by shipment status
                RETURN 
                toLower(sh.status) AS shipmentStatus,
                count(*) AS shipmentCount
                ORDER BY shipmentCount DESC
                """
            }
        ],
        "Efficiency Drivers": [
            {
                "question": "What is the average delivery delay (in days) for shipments by category?",
                "query": """
                // Step 1: Match Shipments and Products
                MATCH (p:Product)-[:SUPPLIED_BY]->(:Supplier)-[:SENDS]->(sh:Shipment)
                MATCH (p)-[:IN_AISLE]->(a:Aisle)
                WHERE sh.expected_delivery_date IS NOT NULL AND sh.actual_delivery_date IS NOT NULL

                // Step 2: Parse dates and calculate delay
                WITH 
                a.aisle AS aisleName,
                split(sh.expected_delivery_date, "/") AS expParts,
                split(sh.actual_delivery_date, "/") AS actParts
                WITH 
                aisleName,
                date({year: toInteger('20' + expParts[2]), month: toInteger(expParts[1]), day: toInteger(expParts[0])}) AS expDate,
                date({year: toInteger('20' + actParts[2]), month: toInteger(actParts[1]), day: toInteger(actParts[0])}) AS actDate

                WITH aisleName, duration.between(expDate, actDate).days AS delayDays
                WHERE delayDays > 0  // only count late deliveries

                // Step 3: Average delay per aisle
                RETURN aisleName, 
                    round(avg(delayDays), 2) AS avgDelayDays,
                    count(*) AS delayedShipments
                ORDER BY avgDelayDays DESC
                """
            },
            {
                "question": "How does product category mix affect on-time delivery?",
                "query": """
                // Step 1: Match shipment and associated product categories
                MATCH (p:Product)-[:SUPPLIED_BY]->(:Supplier)-[:SENDS]->(sh:Shipment)
                MATCH (p)-[:IN_DEPARTMENT]->(d:Department)
                WHERE sh.expected_delivery_date IS NOT NULL AND sh.actual_delivery_date IS NOT NULL

                // Step 2: Parse dates
                WITH 
                d.department AS departmentName,
                split(sh.expected_delivery_date, "/") AS expParts,
                split(sh.actual_delivery_date, "/") AS actParts
                WITH 
                departmentName,
                date({year: toInteger('20' + expParts[2]), month: toInteger(expParts[1]), day: toInteger(expParts[0])}) AS expDate,
                date({year: toInteger('20' + actParts[2]), month: toInteger(actParts[1]), day: toInteger(actParts[0])}) AS actDate

                WITH 
                departmentName,
                CASE WHEN actDate <= expDate THEN "On-Time" ELSE "Delayed" END AS deliveryStatus

                // Step 3: Aggregate per department
                RETURN departmentName, deliveryStatus, count(*) AS shipmentCount
                ORDER BY departmentName, deliveryStatus
                """
            },
            {
                "question": "How does supply chain performance vary with seasonal demand fluctuations?",
                "query": """
                // Step 1: Match shipments with shipment date
                MATCH (p:Product)-[:SUPPLIED_BY]->(:Supplier)-[:SENDS]->(sh:Shipment)
                WHERE sh.shipment_date IS NOT NULL 
                AND sh.expected_delivery_date IS NOT NULL 
                AND sh.actual_delivery_date IS NOT NULL

                // Step 2: Parse dates
                WITH 
                split(sh.shipment_date, "/") AS shipParts,
                split(sh.expected_delivery_date, "/") AS expParts,
                split(sh.actual_delivery_date, "/") AS actParts

                WITH 
                date({year: toInteger('20' + shipParts[2]), month: toInteger(shipParts[1]), day: toInteger(shipParts[0])}) AS shipmentDate,
                date({year: toInteger('20' + expParts[2]), month: toInteger(expParts[1]), day: toInteger(expParts[0])}) AS expDate,
                date({year: toInteger('20' + actParts[2]), month: toInteger(actParts[1]), day: toInteger(actParts[0])}) AS actDate

                WITH 
                shipmentDate.month AS shipmentMonth,
                CASE WHEN actDate <= expDate THEN 1 ELSE 0 END AS onTimeFlag

                // Step 3: Aggregate shipment performance per month
                RETURN shipmentMonth,
                    sum(onTimeFlag) AS onTimeDeliveries,
                    count(*) AS totalShipments,
                    round(toFloat(sum(onTimeFlag)) / count(*) * 100, 2) AS onTimeRatePct
                ORDER BY shipmentMonth
                """
            }
        ]
    },
    "Performance Metrics & Sales KPIs": {
        "Priority stocking & display strategy": [
            {
                "question": "Which products connect the widest variety of departments and have high co-purchase influence?",
                "query": """
                MATCH (p:Product)
                WHERE p.pageRank IS NOT NULL
                MATCH (p)-[:BOUGHTWITH]-(p2:Product)
                MATCH (p)-[:IN_DEPARTMENT]->(d1:Department),
                    (p2)-[:IN_DEPARTMENT]->(d2:Department)
                WHERE d1.department <> d2.department
                WITH p, p.name AS productName, p.pageRank AS pageRankScore,
                    collect(DISTINCT d2.department) AS connectedDepts
                RETURN 
                p.product_id AS productId,
                productName,
                round(pageRankScore, 4) AS pageRank,
                size(connectedDepts) AS crossDeptConnections
                ORDER BY pageRank DESC, crossDeptConnections DESC
                LIMIT 100;
                """
            }
        ],
        "Resource allocation and marketing focus": [
            {
                "question": "Which departments contribute most to overall order volume?",
                "query": """
                // Step 1: Count number of products ordered per department
                MATCH (:Order)-[:CONTAINS]->(p:Product)-[:IN_DEPARTMENT]->(d:Department)
                WITH d.department AS department, COUNT(p) AS orderCount

                // Step 2: Calculate total orders across all departments
                WITH collect({department: department, orderCount: orderCount}) AS deptData
                WITH deptData, reduce(total = 0, row IN deptData | total + row.orderCount) AS totalOrders
                UNWIND deptData AS row

                // Step 3: Compute % contribution of each department
                RETURN 
                row.department AS department, 
                row.orderCount AS orderCount,
                round(toFloat(row.orderCount) / totalOrders * 100, 2) AS pctOfTotalOrders
                ORDER BY orderCount DESC
                LIMIT 21
                """
            }
        ]
    }
}

# Home Page
def home_page():
    st.title("🛒 Retail Analytics Dashboard with Neo4j Integration")
    
    # Connection section
    if not st.session_state.neo4j_connected:
        st.markdown("### First, connect to your Neo4j database:")
        with st.form("neo4j_connection_form"):
            neo4j_uri = st.text_input("Neo4j URI", "bolt://localhost:7689")
            neo4j_user = st.text_input("Username", "neo4j")
            neo4j_password = st.text_input("Password", type="password", value="Password@123")
            submit_button = st.form_submit_button("Connect")
            
            if submit_button:
                if connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password):
                    st.success("Successfully connected to Neo4j!")
                    st.rerun()
    else:
        st.success("Connected to Neo4j")
        st.markdown("### Select a category to explore:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Customer Insights")
            
            if st.button("👥 Customer Lifecycle & Behavior", use_container_width=True):
                navigate_to_category("Customer Lifecycle & Behavior")
                
            if st.button("🔁 Reorder Behavior & Loyalty Drivers", use_container_width=True):
                navigate_to_category("Reorder Behavior & Loyalty Drivers")
                
            if st.button("🛍️ Product Affinity, Graphs & Recommendations", use_container_width=True):
                navigate_to_category("Product Affinity, Graphs & Recommendations")
                
        with col2:
            st.markdown("#### Operations & Performance")
            
            if st.button("📉 Demand Trends & Seasonality", use_container_width=True):
                navigate_to_category("Demand Trends & Seasonality")
                
            if st.button("🚚 Supply Chain & Delivery Performance", use_container_width=True):
                navigate_to_category("Supply Chain & Delivery Performance")
                
            if st.button("📊 Performance Metrics & Sales KPIs", use_container_width=True):
                navigate_to_category("Performance Metrics & Sales KPIs")
        
        # Display some summary stats from Neo4j
        st.markdown("---")
        st.subheader("Database Overview")
        
        try:
            col1, col2, col3, col4 = st.columns(4)
            
            # Customers count
            customer_count_df = run_neo4j_query("MATCH (c:User) RETURN count(c) as customer_count")
            if customer_count_df is not None and not customer_count_df.empty:
                col1.metric("Customers", f"{customer_count_df.iloc[0]['customer_count']:,}")
            
            # Orders count
            order_count_df = run_neo4j_query("MATCH (o:Order) RETURN count(o) as order_count")
            if order_count_df is not None and not order_count_df.empty:
                col2.metric("Orders", f"{order_count_df.iloc[0]['order_count']:,}")
            
            # Products count
            product_count_df = run_neo4j_query("MATCH (p:Product) RETURN count(p) as product_count")
            if product_count_df is not None and not product_count_df.empty:
                col3.metric("Products", f"{product_count_df.iloc[0]['product_count']:,}")
            
            # Departments count
            dept_count_df = run_neo4j_query("MATCH (d:Department) RETURN count(d) as department_count")
            if dept_count_df is not None and not dept_count_df.empty:
                col4.metric("Departments", f"{dept_count_df.iloc[0]['department_count']:,}")

        except Exception as e:
           st.warning(f"Unable to load metrics: {e}")

# Category Page
def category_page():
    category = st.session_state.category
    
    st.title(f"{category}")
    
    # If no section is selected, show the section selection
    if st.session_state.section is None:
        st.markdown("### Select a section to explore:")
        
        # Create columns based on number of sections
        num_sections = len(questions[category])
        cols = st.columns(min(num_sections, 3))
        
        # Distribute sections across columns
        section_list = list(questions[category].keys())
        for i, section in enumerate(section_list):
            col_idx = i % min(num_sections, 3)
            with cols[col_idx]:
                if st.button(f"{section}", use_container_width=True, key=f"section_{section}"):
                    navigate_to_section(section)
    
    # If a section is selected, show the questions for that section
    else:
        section = st.session_state.section
        st.subheader(f"{section}")
        
        # Display each question in the section
        for i, question_data in enumerate(questions[category][section]):
            question = question_data["question"]
            query = question_data["query"]
            
            st.markdown(f"**- {question}**")
            
            # Execute Neo4j query button
            if st.button(f"Run Analysis", key=f"run_{category}_{section}_{i}"):
                with st.spinner("Executing query..."):
                    result_df = run_neo4j_query(query)
                    
                    if result_df is not None and not result_df.empty:
                        st.session_state.responses[f"{category} | {section} | {question}"] = result_df
                        
                        # Show the results as a table
                        st.dataframe(result_df)

                        # Only show pie chart for the specific cart behavior question
                        if question.strip().startswith("How does the average cart size evolve over a customer's lifetime (by order number)?"):
                            if "CartBehavior" in result_df.columns:
                                behavior_counts = result_df["CartBehavior"].value_counts().reset_index()
                                behavior_counts.columns = ["CartBehavior", "Count"]

                                fig = px.pie(
                                    behavior_counts,
                                    names="CartBehavior",
                                    values="Count",
                                    title="Customer Cart Behavior Distribution",
                                    color_discrete_sequence=px.colors.qualitative.Set2
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("What's the typical progression of departments that new customers explore over their first 5 orders?"):
                            if not result_df.empty and {"orderNum", "departmentName", "freq"}.issubset(result_df.columns):
                                fig = px.bar(
                                    result_df,
                                    x="orderNum",
                                    y="freq",
                                    color="departmentName",
                                    title="Department Exploration Across First 5 Orders",
                                    labels={"orderNum": "Order Number", "freq": "Number of Products"}
                                )
                                fig.update_layout(barmode="stack", xaxis=dict(dtick=1))
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("How many aisles do new clients usually browse throughout their first 5 orders?"):
                            if "aisleName" in result_df.columns and "frequency" in result_df.columns:
                                top_aisles = result_df.sort_values("frequency", ascending=False).head(20)
                                fig = px.bar(
                                    top_aisles,
                                    x="frequency",
                                    y="aisleName",
                                    orientation="h",
                                    title="Top 20 Most Frequently Browsed Aisles (First 5 Orders)",
                                    labels={"frequency": "Browse Frequency", "aisleName": "Aisle"},
                                    color_discrete_sequence=["#F8766D"]
                                )
                                fig.update_layout(yaxis=dict(autorange="reversed"))
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("How does order hour preference evolve for departments over time?"):
                            if {"hour", "department", "freq"}.issubset(result_df.columns):
                                pivot_df = result_df.pivot(index="department", columns="hour", values="freq").fillna(0)

                                fig = px.imshow(
                                    pivot_df,
                                    labels=dict(x="Hour of Day", y="Department", color="Frequency"),
                                    x=pivot_df.columns,
                                    y=pivot_df.index,
                                    color_continuous_scale="Plasma",
                                    title="Hourly Frequency of Orders by Department"
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("How does customer/basket share vary across product categories over time?"):
                            if {"dayOfWeek", "department", "basketSharePct"}.issubset(result_df.columns):
                                pivot_df = result_df.pivot(index="department", columns="dayOfWeek", values="basketSharePct").fillna(0)

                                fig = px.imshow(
                                    pivot_df,
                                    labels=dict(x="Day of Week", y="Department", color="Basket Share (%)"),
                                    x=pivot_df.columns,
                                    y=pivot_df.index,
                                    color_continuous_scale="YlGnBu",
                                    text_auto=True,
                                    title="Basket Share by Department Across Days of Week (Heatmap)"
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("How does customer retention vary over time based on their preferred shopping department?"):
                            if {"departmentSegment", "orderNum", "customers"}.issubset(result_df.columns):
                                # Pivot to get orderNum on x-axis and department segments as separate lines
                                pivot_df = result_df.pivot(index="orderNum", columns="departmentSegment", values="customers").fillna(0)

                                # Normalize customer count to percentage retention from first order (orderNum = 1) for each department
                                for col in pivot_df.columns:
                                    if pivot_df.loc[1, col] > 0:
                                        pivot_df[col] = pivot_df[col] / float(pivot_df.loc[1, col]) * 100

                                fig = px.line(
                                    pivot_df,
                                    x=pivot_df.index,
                                    y=pivot_df.columns,
                                    title="Customer Retention Over Orders by Preferred Department",
                                    labels={"value": "Customer Retention (%)", "orderNum": "Order Number"},
                                )
                                fig.update_layout(legend_title="Preferred Department")
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Which products play a central role in the early purchasing patterns of retained customers?"):
                            if {"productName", "pageRankScore"}.issubset(result_df.columns):
                                fig = px.bar(
                                    result_df.sort_values("pageRankScore", ascending=True),
                                    x="pageRankScore",
                                    y="productName",
                                    orientation="h",
                                    title="Top Central Products in Early Orders (PageRank)",
                                    labels={"productName": "Product", "pageRankScore": "PageRank Score"}
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Can we identify distinct customer segments based on ordering day/time patterns?"):
                            if {"timeSegment", "userCount"}.issubset(result_df.columns):
                                fig = px.pie(
                                    result_df,
                                    names="timeSegment",
                                    values="userCount",
                                    title="User Segments by Preferred Order Time"
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Which products are most frequently transitioned into by loyal customers?"):
                            if {"productName", "retainedUsers"}.issubset(result_df.columns):
                                fig = px.bar(
                                    result_df.sort_values("retainedUsers", ascending=False),
                                    x="productName",
                                    y="retainedUsers",
                                    title="Top Retention Products for Loyal Users (80% Order Presence)",
                                    labels={"productName": "Product", "retainedUsers": "# of Loyal Users"},
                                    color="retainedUsers",
                                    color_continuous_scale="Agsunset"
                                )
                                fig.update_layout(xaxis_tickangle=45)
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Which products serve as 'gateway purchases' that lead to exploration of new departments?"):
                            if {"gatewayProduct", "newDeptCount"}.issubset(result_df.columns):
                                fig = px.bar(
                                    result_df.sort_values("newDeptCount", ascending=True),
                                    x="newDeptCount",
                                    y="gatewayProduct",
                                    orientation="h",
                                    title="Top Gateway Products Leading to New Department Exploration",
                                    labels={"newDeptCount": "New Departments Reached", "gatewayProduct": "Gateway Product"},
                                    color="newDeptCount",
                                    color_continuous_scale="Viridis"
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Are customers reordering items popular with others in their first order?"):
                                top_products = result_df.sort_values("percentInFirstOrders", ascending=False)

                                fig = px.bar(
                                    top_products,
                                    x="percentInFirstOrders",
                                    y="productName",
                                    orientation="h",
                                    title="Top Products by First Order Inclusion (%)",
                                    labels={
                                        "percentInFirstOrders": "% Included in First Orders",
                                        "productName": "Product"
                                    },
                                    color="totalOrders",
                                    color_continuous_scale="Tealgrn"
                                )
                                fig.update_layout(
                                    yaxis=dict(autorange="reversed"),
                                    xaxis=dict(ticksuffix="%"),
                                    legend_title="Total Orders"
                                )
                                st.plotly_chart(fig, use_container_width=True)


                        if question.strip().startswith("How does reorder frequency vary with days_since_prior_order for frequently purchased items?"):
                                fig = px.bar(
                                    result_df.sort_values("avgDaysBetweenReorders"),
                                    x="avgDaysBetweenReorders",
                                    y="productName",
                                    orientation="h",
                                    title="Average Days Between Reorders (Top Reordered Products)",
                                    labels={"avgDaysBetweenReorders": "Avg Days Between Reorders", "productName": "Product"},
                                    color="avgDaysBetweenReorders",
                                    color_continuous_scale="Reds"
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("What's the relationship between a product's department and reorder likelihood?"):
                                fig = px.bar(
                                    result_df,
                                    x="department",
                                    y="reorderRatePct",
                                    title="Reorder Rate by Department",
                                    labels={"department": "Department", "reorderRatePct": "Reorder Rate (%)"},
                                    color="reorderRatePct",
                                    color_continuous_scale="Purples"
                                )
                                st.plotly_chart(fig, use_container_width=True)
                       
                        if question.strip().startswith("Which product categories have the highest uplift in sales?") and "product" in result_df.columns:
                                    # Sort top uplift products for better visibility
                                    top_products = result_df.sort_values("avgUpliftPct", ascending=False).head(20)

                                    fig = px.bar(
                                        top_products,
                                        x="avgUpliftPct",
                                        y="product",
                                        orientation="h",
                                        text="avgUpliftPct",
                                        title="Top 20 Products with Highest Sales Uplift (Late vs Early Orders)",
                                        labels={
                                            "avgUpliftPct": "Avg Sales Uplift (%)",
                                            "product": "Product Name"
                                        },
                                        color="avgUpliftPct",
                                        color_continuous_scale="Tealgrn"
                                    )

                                    fig.update_layout(
                                        yaxis=dict(autorange="reversed"),
                                        xaxis=dict(title="Uplift %"),
                                        margin=dict(l=120, r=40, t=60, b=40)
                                    )

                                    st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("What are the top product pairings based on co-purchase frequency?"):
                                if {"productA", "productB", "timesBoughtTogether"}.issubset(result_df.columns):
                                    result_df["pair"] = result_df.apply(lambda row: f"{row['productA']} + {row['productB']}", axis=1)
                                    fig = px.bar(
                                        result_df[::-1],  # reverse for top-down bar chart
                                        x="timesBoughtTogether",
                                        y="pair",
                                        orientation="h",
                                        title="Top Product Co-Purchase Pairs by Frequency",
                                        labels={"timesBoughtTogether": "Times Bought Together", "pair": "Product Pair"},
                                        color="timesBoughtTogether",
                                        color_continuous_scale="viridis"
                                    )
                                    fig.update_layout(yaxis=dict(autorange="reversed"))
                                    st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("What products have the highest cross-department purchase correlation?") and not result_df.empty:
                            fig = px.strip(
                                result_df,
                                x="similarityScore",
                                y="deptA",
                                color="deptB",
                                hover_data=["productA", "productB"],
                                title="Cross-Department Product Similarity Scores",
                                labels={
                                    "similarityScore": "Similarity Score",
                                    "deptA": "Department A",
                                    "deptB": "Department B"
                                },
                                stripmode="overlay"
                            )
                            fig.update_traces(marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')))
                            fig.update_layout(
                                yaxis=dict(title="Department A"),
                                xaxis=dict(title="Product Similarity Score"),
                                legend_title="Compared with Dept B",
                                margin=dict(l=60, r=40, t=60, b=40)
                            )
                            st.plotly_chart(fig, use_container_width=True)


                        if question.strip().startswith("Which items act as a 'bridge' between different product communities?") and "productName" in result_df.columns:
                                fig = px.bar(
                                    result_df,
                                    x="betweennessScore",
                                    y="productName",
                                    color="sourceCommunity",
                                    orientation="h",
                                    title="Top Products Bridging Different Communities",
                                    labels={
                                        "betweennessScore": "Betweenness Centrality",
                                        "productName": "Product",
                                        "sourceCommunity": "Community"
                                    },
                                    height=600
                                )
                                fig.update_layout(
                                    yaxis=dict(autorange="reversed"),
                                    legend_title="Community",
                                    margin=dict(l=120, r=40, t=60, b=40)
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Do high-centrality products (high betweenness) make better recommendations than low-centrality ones?") and "centralityGroup" in result_df.columns:
                            fig = px.pie(
                                result_df,
                                names="centralityGroup",
                                values="totalOrders",
                                title="Recommendation Uptake by Product Centrality",
                                color_discrete_sequence=["#1f77b4", "#ff7f0e"],  # same color palette
                                hole=0.4  # donut style for better readability
                            )
                            fig.update_traces(textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)


                        if question.strip().startswith("Which product categories exhibit strong day-of-week effects?"):
                                fig = px.bar(
                                    result_df,
                                    x="department",
                                    y="volatilityPct",
                                    title="Seasonality (Volatility %) Across Product Categories",
                                    labels={"volatilityPct": "Volatility (%)", "department": "Department"},
                                    color="volatilityPct",
                                    color_continuous_scale="Oranges"
                                )
                                fig.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig, use_container_width=True)
                    
                        if question.strip().startswith("What is the day-of-week effect on order size across different product categories?") and {"department", "highestCartDay", "lowestCartDay", "highestAvgCartSize", "lowestAvgCartSize"}.issubset(result_df.columns):
                            # Melt the dataframe for easier grouped bar plotting
                            melted = pd.melt(result_df, 
                                            id_vars=["department"], 
                                            value_vars=["highestAvgCartSize", "lowestAvgCartSize"], 
                                            var_name="Metric", 
                                            value_name="AvgCartSize")

                            # Use mapped day names directly (no need to map now)
                            melted["CartDay"] = melted.apply(lambda row: 
                                result_df.loc[result_df["department"] == row["department"], 
                                "highestCartDay" if row["Metric"] == "highestAvgCartSize" else "lowestCartDay"].values[0], axis=1)

                            fig = px.bar(
                                melted,
                                x="department",
                                y="AvgCartSize",
                                color="Metric",
                                barmode="group",
                                hover_data=["CartDay"],
                                title="Highest and Lowest Avg Cart Size Day by Department",
                                labels={"AvgCartSize": "Avg Cart Size", "department": "Department", "Metric": "Day Type"}
                            )
                            fig.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)


                        if question.strip().startswith("Which products connect the widest variety of departments and have high co-purchase influence?") and "pageRank" in result_df.columns:
                            fig = px.scatter(
                                result_df,
                                x="crossDeptConnections",
                                y="pageRank",
                                size="pageRank",
                                color="pageRank",
                                hover_name="productName",
                                title="Influential Cross-Department Products (PageRank vs Connections)",
                                labels={
                                    "crossDeptConnections": "# Departments Reached",
                                    "pageRank": "PageRank Influence"
                                },
                                size_max=40,
                                color_continuous_scale="Viridis"
                            )
                            fig.update_layout(xaxis_title="Cross-Department Reach", yaxis_title="Product Influence (PageRank)")
                            st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("Which departments contribute most to overall order volume?"):
                                
                                if {"department", "orderCount", "pctOfTotalOrders"}.issubset(result_df.columns):
                                    fig = px.pie(
                                        result_df,
                                        names="department",
                                        values="orderCount",
                                        title="Department-wise Contribution to Total Order Volume",
                                        hover_data=["pctOfTotalOrders"],
                                        hole=0.4  # Donut style
                                    )
                                    fig.update_traces(textinfo="percent+label")
                                    st.plotly_chart(fig, use_container_width=True)
                        
                        if question.strip().startswith("What proportion of shipments are in transit, delivered and dispatched?"):
                            fig = px.pie(
                                result_df,
                                names="shipmentStatus",
                                values="shipmentCount",
                                title="Shipment Status Distribution",
                                color_discrete_sequence=px.colors.qualitative.Set3
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("What is the average delivery delay (in days) for shipments by category?") and "aisleName" in result_df.columns:
                            fig = px.bar(
                                result_df,
                                x="aisleName",
                                y="avgDelayDays",
                                title="Average Delivery Delay by Aisle",
                                labels={"aisleName": "Aisle", "avgDelayDays": "Average Delay (Days)"},
                                color="avgDelayDays",
                                color_continuous_scale="Reds"
                            )
                            fig.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)

                        if question.strip().startswith("How does product category mix affect on-time delivery?") and "departmentName" in result_df.columns:
                            fig = px.bar(
                                result_df,
                                x="departmentName",
                                y="shipmentCount",
                                color="deliveryStatus",
                                barmode="group",
                                title="Shipment Delivery Status by Department",
                                labels={"departmentName": "Department", "shipmentCount": "Shipment Count"},
                                color_discrete_sequence=px.colors.qualitative.Set2
                            )
                            fig.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)
    
                        if question.strip().startswith("How does supply chain performance vary with seasonal demand fluctuations?") and "shipmentMonth" in result_df.columns:
                            fig = px.line(
                                result_df,
                                x="shipmentMonth",
                                y="onTimeRatePct",
                                title="Monthly Supply Chain On-Time Delivery Rate",
                                labels={"shipmentMonth": "Month", "onTimeRatePct": "On-Time Delivery (%)"},
                                markers=True
                            )
                            fig.update_traces(marker=dict(size=8))
                            st.plotly_chart(fig, use_container_width=True)
    
        # Button to go back to section selection
        if st.button("Back to Sections", key="back_to_sections"):
            st.session_state.section = None
            st.rerun()

# Sidebar navigation
st.sidebar.title("Navigation")
if st.sidebar.button("Home", key="sidebar_home"):
    navigate_to_home()

if st.session_state.neo4j_connected:
    st.sidebar.markdown("---")
    st.sidebar.subheader("Categories")
    
    for category in questions.keys():
        if st.sidebar.button(category, key=f"sidebar_{category}"):
            navigate_to_category(category)
            
        # If category is selected, show sections in sidebar
        if st.session_state.category == category and st.session_state.current_page == "Category":
            for section in questions[category].keys():
                if st.sidebar.button(f"  • {section}", key=f"sidebar_{category}_{section}"):
                    st.session_state.section = section
    
    st.sidebar.markdown("---")
    
    # Disconnect button
    if st.sidebar.button("Disconnect from Neo4j"):
        if st.session_state.neo4j_driver:
            try:
                st.session_state.neo4j_driver.close()
            except:
                pass
        st.session_state.neo4j_connected = False
        st.session_state.neo4j_driver = None
        st.sidebar.success("Disconnected from Neo4j")
        st.rerun()
    
    # Export results
    st.sidebar.subheader("Export Results")
    export_format = st.sidebar.selectbox("Format:", ["CSV", "Excel"])
    if st.sidebar.button("Export All Results"):
        if st.session_state.responses:
            all_data = {}
            for key, value in st.session_state.responses.items():
                if isinstance(value, pd.DataFrame):
                    if not key.endswith("_notes"):
                        sheet_name = key.split(" | ")[-1][:31]  # Excel sheet name length limit
                        all_data[sheet_name] = value
            
            if export_format == "CSV":
                for sheet_name, df in all_data.items():
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.sidebar.download_button(
                        label=f"Download {sheet_name}",
                        data=csv,
                        file_name=f"{sheet_name}.csv",
                        mime='text/csv',
                    )
            else:
                try:
                    import io
                    import xlsxwriter
                    output = io.BytesIO()
                    
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        for sheet_name, df in all_data.items():
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    st.sidebar.download_button(
                        label=f"Download All Results (Excel)",
                        data=output.getvalue(),
                        file_name="retail_analytics_results.xlsx",
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    )
                except Exception as e:
                    st.sidebar.error(f"Error creating Excel file: {str(e)}")
        else:
            st.sidebar.warning("No results to export yet!")

# Render the current page
if st.session_state.current_page == "Home":
    home_page()
elif st.session_state.current_page == "Category":
    category_page()

# Clean up on app exit
def on_exit():
    if st.session_state.neo4j_driver:
        st.session_state.neo4j_driver.close()

# Register the cleanup function
import atexit
atexit.register(on_exit)