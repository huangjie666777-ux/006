namespace EventRelay.Api.Domain;

public sealed record ApiError(string Code, string Message, int Status)
{
    public static ApiError BadRequest(string code, string message) => new(code, message, StatusCodes.Status400BadRequest);
    public static ApiError Conflict(string code, string message) => new(code, message, StatusCodes.Status409Conflict);

    public IResult ToHttpResult() =>
        Results.Json(new { error = new { code = Code, message = Message } }, statusCode: Status);
}

