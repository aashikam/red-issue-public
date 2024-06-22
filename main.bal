import ballerina/graphql;
import ballerina/io;
import ballerina/time;
import ballerinax/aws.redshift.driver as _;

configurable string & readonly jdbcUrl = ?;
configurable string & readonly user = ?;
configurable string & readonly password = ?;

configurable int & readonly maxOpenConnections = 80;
configurable int & readonly minIdleConnections = 20;

// sql:ConnectionPool pool = {
//     maxOpenConnections: maxOpenConnections,
//     minIdleConnections: minIdleConnections
// };

// final redshift:Client dbClient = check new (password = password, user = user, url = jdbcUrl, connectionPool = pool);

// configurable int port = 9090;

// isolated int i = 0;

// @graphql:ServiceConfig {
//     interceptors: [new LogInterceptor()]
// }
service /graphql on new graphql:Listener(9090, timeout = 100) {
    private final record {}[] csvData;

    isolated function init() returns error? {
        self.csvData = check io:fileReadCsv("./86861-1719067004344.csv");
    }

    isolated resource function get greeting() returns string|error {
        stream<record {}> resultSet = self.csvData.toStream();
        time:Utc startQ = time:utcNow(3);
        record {}[] data = [];

        record {}?|error next = resultSet.next();

        while next is record {} {
            data.push(next);
            next = resultSet.next();
        }
        time:Utc end2 = time:utcNow(3);
        io:println("loop: " + (time:utcDiffSeconds(end2, startQ) * 1000).toString());

        return data[0].toString();
    }
}

// time:Utc startQ = time:utcNow(3);
//         string uid = uuid:createType1AsString();
//         record {}[]|error assetAllocationV2 = check getAssetAllocationV2(uid);
//         if assetAllocationV2 is error {
//             log:printError("ERROR: ", assetAllocationV2);
//             return assetAllocationV2;
//         } else {
//             time:Utc end2 = time:utcNow(3);
//             io:println("graphql returning request " + uid + ": " + (time:utcDiffSeconds(end2, startQ) * 1000).toString());
//             return assetAllocationV2[0].toJsonString();
//         }

// public isolated function getAssetAllocationV2(string accountId) returns record {}[]|error {
//     transaction {

//         sql:CursorOutParameter curResults = new;
//         time:Utc startQ = time:utcNow(3);
//         sql:ProcedureCallResult result = check dbClient->call(`{CALL GetUserInfo(${curResults})}`);
//         time:Utc end1 = time:utcNow(3);

//         io:println("dbClient->call() Duration for request " + accountId + ": " + (time:utcDiffSeconds(end1, startQ) * 1000).toString());

//         stream<record {}, sql:Error?> resultSet = curResults.get();

//         time:Utc end2 = time:utcNow(3);
//         io:println("curResults.get() Duration for request " + accountId + ": " + (time:utcDiffSeconds(end2, startQ) * 1000).toString());

//         record {}[] data = [];

//         record {}?|error next = resultSet.next();

//         while next is record {} {
//             data.push(next);
//             next = resultSet.next();
//         }

//         time:Utc end3 = time:utcNow(3);
//         io:println("Loop Duration for request " + accountId + ": " + (time:utcDiffSeconds(end3, startQ) * 1000).toString());

//         check result.close();

//         check commit;
//         return data;

//     }

// }

