import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = { wrapNumbers: false };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    console.log("[EVENT]", JSON.stringify(event));

    // 1) 从 pathParameters 获取 movieId
    const pathParameters = event.pathParameters;
    const movieId = pathParameters?.movieId ? parseInt(pathParameters.movieId) : undefined;
    if (!movieId) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Missing movieId in path" }),
      };
    }

    // 2) 如果 queryStringParameters.cast === "true"，说明要返回演员列表
    const castParam = event.queryStringParameters?.cast;
    const includeCast = (castParam === "true");

    // 3) 首先查询 Movies 表 (TABLE_NAME)
    const movieResult = await ddbDocClient.send(
      new GetCommand({
        TableName: process.env.TABLE_NAME,  // e.g. "Movies"
        Key: { id: movieId },
      })
    );
    if (!movieResult.Item) {
      return {
        statusCode: 404,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: `Movie ${movieId} not found` }),
      };
    }

    // 4) 如果需要演员表，再查询 CAST_TABLE_NAME
    let castData: any[] = [];
    if (includeCast) {
      const castOutput = await ddbDocClient.send(
        new QueryCommand({
          TableName: process.env.CAST_TABLE_NAME,  // e.g. "MovieCast"
          KeyConditionExpression: "movieId = :m",
          ExpressionAttributeValues: {
            ":m": movieId,
          },
        })
      );
      castData = castOutput.Items ?? [];
    }

    // 5) 构造返回
    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        data: movieResult.Item,
        cast: castData, // 如果不需要，则空数组
      }),
    };

  } catch (error: any) {
    console.error(error);
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error: error.message }),
    };
  }
};
