/**
 * Execution in WSL / Terminal:
 *   mongosh "mongodb://gitlytics:admin123@localhost:27018/gitlytics?authSource=admin" < mongodb/queries.js
 * Or run interactively line-by-line in mongosh or Compass.
 */

// ============================================================================
// 0. ENVIRONMENT & COLLECTION VERIFICATION
// ============================================================================
use gitlytics;

print("\n--- 0. COLLECTION STATS ---");
print("Database: " + db.getName());
print("Total Documents in events: " + db.events.countDocuments());
printjson(db.events.stats({ scale: 1024 * 1024 })); // size in MB

// ============================================================================
// 1. CRUD OPERATIONS (Create, Read, Update, Delete)
// ============================================================================
print("\n--- 1. CRUD: INSERT (CREATE) ---");
// Insert a sample synthetic event
const insertResult = db.events.insertOne({
    id: "99999999999",
    type: "WatchEvent",
    actor: {
        id: 100001,
        login: "demo-analyst",
        display_login: "demo-analyst",
        url: "https://api.github.com/users/demo-analyst"
    },
    repo: {
        id: 500001,
        name: "gitlytics/gitlytics-core",
        url: "https://api.github.com/repos/gitlytics/gitlytics-core"
    },
    payload: {
        action: "started"
    },
    public: true,
    created_at: "2024-09-28T12:00:00Z"
});
printjson(insertResult);

print("\n--- 1. CRUD: FIND (READ) ---");
// Read the inserted document with projection
const foundDoc = db.events.findOne(
    { id: "99999999999" },
    { _id: 0, id: 1, type: 1, "actor.login": 1, "repo.name": 1, created_at: 1 }
);
printjson(foundDoc);

print("\n--- 1. CRUD: UPDATE ---");
// Update payload and add a verified tag
const updateResult = db.events.updateOne(
    { id: "99999999999" },
    {
        $set: { "payload.action": "starred_verified", verified_by: "lab_tester" },
        $currentDate: { updated_at: true }
    }
);
printjson(updateResult);

print("\n--- 1. CRUD: DELETE ---");
// Delete the sample document
const deleteResult = db.events.deleteOne({ id: "99999999999" });
printjson(deleteResult);


// ============================================================================
// 2. INDEXING & QUERY OPTIMIZATION (EXPLAIN PLANS)
// ============================================================================
print("\n--- 2. PERFORMANCE BEFORE INDEX (COLLSCAN) ---");
// Query a specific repo before indexing on repo.name
const explainBefore = db.events.find({ "repo.name": "freeCodeCamp/freeCodeCamp" })
    .explain("executionStats");

print("Execution Stage: " + explainBefore.executionStats.executionStages.stage);
print("Total Docs Examined: " + explainBefore.executionStats.totalDocsExamined);
print("Execution Time (ms): " + explainBefore.executionStats.executionTimeMillis);

print("\n--- CREATING INDEXES ---");
// Create single-field and compound indexes
db.events.createIndex({ "type": 1 }, { name: "idx_type" });
db.events.createIndex({ "repo.name": 1 }, { name: "idx_repo_name" });
db.events.createIndex({ "actor.login": 1, "created_at": -1 }, { name: "idx_actor_created" });
db.events.createIndex({ "created_at": 1 }, { name: "idx_created_at" });
printjson(db.events.getIndexes());

print("\n--- PERFORMANCE AFTER INDEX (IXSCAN) ---");
// Same query after index
const explainAfter = db.events.find({ "repo.name": "freeCodeCamp/freeCodeCamp" })
    .explain("executionStats");

print("Execution Stage: " + explainAfter.executionStats.executionStages.stage);
print("Total Docs Examined: " + explainAfter.executionStats.totalDocsExamined);
print("Execution Time (ms): " + explainAfter.executionStats.executionTimeMillis);


// ============================================================================
// 3. AGGREGATION PIPELINES (5 ANALYTICS PIPELINES)
// ============================================================================

// ----------------------------------------------------------------------------
// Pipeline 1: Event Type Distribution & Percentage Share
// ----------------------------------------------------------------------------
print("\n--- AGGREGATION 1: EVENT TYPE SHARE ---");
const agg1 = db.events.aggregate([
    {
        $group: {
            _id: "$type",
            count: { $sum: 1 }
        }
    },
    {
        $setWindowFields: {
            output: {
                totalEvents: { $sum: "$count" }
            }
        }
    },
    {
        $project: {
            _id: 0,
            event_type: "$_id",
            count: 1,
            share_pct: {
                $round: [{ $multiply: [{ $divide: ["$count", "$totalEvents"] }, 100] }, 2]
            }
        }
    },
    { $sort: { count: -1 } }
]).toArray();
printjson(agg1);


// ----------------------------------------------------------------------------
// Pipeline 2: Top 15 Starred Repositories (WatchEvent)
// ----------------------------------------------------------------------------
print("\n--- AGGREGATION 2: TOP 15 STARRED REPOSITORIES ---");
const agg2 = db.events.aggregate([
    { $match: { type: "WatchEvent" } },
    {
        $group: {
            _id: "$repo.name",
            stars_count: { $sum: 1 },
            distinct_starrers: { $addToSet: "$actor.login" }
        }
    },
    {
        $project: {
            _id: 0,
            repo: "$_id",
            total_stars: "$stars_count",
            unique_users: { $size: "$distinct_starrers" }
        }
    },
    { $sort: { unique_users: -1 } },
    { $limit: 15 }
]).toArray();
printjson(agg2);


// ----------------------------------------------------------------------------
// Pipeline 3: Commit Volume & Average Message Length per Repository (PushEvent)
// ----------------------------------------------------------------------------
print("\n--- AGGREGATION 3: PUSH COMMIT ANALYTICS ---");
const agg3 = db.events.aggregate([
    { $match: { type: "PushEvent", "payload.commits": { $exists: true, $ne: [] } } },
    { $unwind: "$payload.commits" },
    {
        $project: {
            repo: "$repo.name",
            msg_length: { $strLenCP: { $ifNull: ["$payload.commits.message", ""] } }
        }
    },
    {
        $group: {
            _id: "$repo",
            total_commits: { $sum: 1 },
            avg_msg_length: { $avg: "$msg_length" }
        }
    },
    {
        $project: {
            _id: 0,
            repo: "$_id",
            total_commits: 1,
            avg_msg_length: { $round: ["$avg_msg_length", 1] }
        }
    },
    { $sort: { total_commits: -1 } },
    { $limit: 10 }
]).toArray();
printjson(agg3);


// ----------------------------------------------------------------------------
// Pipeline 4: Bot vs. Human Activity Breakdown
// ----------------------------------------------------------------------------
print("\n--- AGGREGATION 4: BOT VS HUMAN TRAFFIC ---");
const agg4 = db.events.aggregate([
    {
        $project: {
            type: 1,
            is_bot: {
                $regexMatch: {
                    input: "$actor.login",
                    regex: "\\[bot\\]$",
                    options: "i"
                }
            }
        }
    },
    {
        $group: {
            _id: { type: "$type", is_bot: "$is_bot" },
            event_count: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            event_type: "$_id.type",
            actor_category: { $cond: ["$_id.is_bot", "BOT", "HUMAN"] },
            event_count: 1
        }
    },
    { $sort: { event_type: 1, actor_category: 1 } }
]).toArray();
printjson(agg4);


// ----------------------------------------------------------------------------
// Pipeline 5: Issues Event Lifecycle Breakdown (Opened vs Closed vs Reopened)
// ----------------------------------------------------------------------------
print("\n--- AGGREGATION 5: ISSUES LIFECYCLE BREAKDOWN ---");
const agg5 = db.events.aggregate([
    { $match: { type: "IssuesEvent" } },
    {
        $group: {
            _id: "$payload.action",
            total_actions: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            action: "$_id",
            total_actions: 1
        }
    },
    { $sort: { total_actions: -1 } }
]).toArray();
printjson(agg5);