import ballerina/graphql;
import ballerina/io;
import ballerina/sql;
import ballerina/time;
import ballerina/log;
import ballerinax/aws.redshift;
import ballerinax/aws.redshift.driver as _;
import ballerina/uuid;

configurable string & readonly jdbcUrl = ?;
configurable string & readonly user = ?;
configurable string & readonly password = ?;

configurable int & readonly maxOpenConnections = 80;
configurable int & readonly minIdleConnections = 20;

sql:ConnectionPool pool = {
    maxOpenConnections: maxOpenConnections,
    minIdleConnections: minIdleConnections
};

final redshift:Client dbClient = check new (password = password, user = user, url = jdbcUrl, connectionPool = pool);

configurable int port = 9090;

isolated int i = 0;

// @graphql:ServiceConfig {
//     interceptors: [new LogInterceptor()]
// }
service /graphql on new graphql:Listener(9090, timeout = 100) {
    isolated resource function get greeting() returns string|error {
        time:Utc startQ = time:utcNow(3);
        string uid = uuid:createType1AsString();
        record {}[]|error assetAllocationV2 = check getAssetAllocationV2(uid);
        if assetAllocationV2 is error {
            log:printError("ERROR: ", assetAllocationV2);
            return assetAllocationV2;
        } else {
            time:Utc end2 = time:utcNow(3);
            io:println("graphql returning request " + uid + ": " + (time:utcDiffSeconds(end2, startQ) * 1000).toString());
            return assetAllocationV2.toJsonString();
        }
    }
}

public isolated function getAssetAllocationV2(string accountId) returns record {}[]|error {
    transaction {

        sql:CursorOutParameter curResults = new;
        time:Utc startQ = time:utcNow(3);
        sql:ProcedureCallResult result = check dbClient->call(`{CALL GetUserInfo(${curResults})}`);
        time:Utc end1 = time:utcNow(3);

        io:println("dbClient->call() Duration for request " + accountId + ": " + (time:utcDiffSeconds(end1, startQ) * 1000).toString());

        stream<record {}, sql:Error?> resultSet = curResults.get();

        time:Utc end2 = time:utcNow(3);
        io:println("curResults.get() Durationfor request " + accountId + ": " + (time:utcDiffSeconds(end2, startQ) * 1000).toString());

        record {}[] data = check from record {} user in resultSet

            select user;

        time:Utc end3 = time:utcNow(3);
        io:println("Loop Durationfor request " + accountId + ": " + (time:utcDiffSeconds(end3, startQ) * 1000).toString());

        check result.close();

        check commit;
        return data;

    }

}

// public isolated function getAssetAllocationV2(string accountId) returns record {}[]|error {
//     //transaction {
//     time:Utc startQ = time:utcNow(3);
//     //sql:CursorOutParameter curResults = new;

//     sql:VarcharValue varcharValue = new ("mytempresult");
//     sql:InOutParameter param = new(varcharValue);
//     sql:ProcedureCallResult result = check dbClient->call(`CALL GetUserInfo2(${param});`);

//     sql:ParameterizedQuery query = `SELECT * FROM mytempresult;`;
//     stream<record {}, sql:Error?> resultSet = dbClient->query(query);
//     record {}[] data = check from record {} user in resultSet
//         select user;

//     check result.close();
//     time:Utc endQ = time:utcNow(3);
//     if (data.length() != 0) {
//         io:println("Query Duration: " + (time:utcDiffSeconds(endQ, startQ) * 1000).toString());
//     }
//     // check commit;
//     return data;
//     //}

// }
