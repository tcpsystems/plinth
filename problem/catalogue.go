package problem

// Catalogue slugs, v1 (errors.yaml).
const (
	CodeInvalid             = "invalid"
	CodeUnauthorized        = "unauthorized"
	CodeForbidden           = "forbidden"
	CodeNotFound            = "not_found"
	CodeConflict            = "conflict"
	CodeIdempotencyMismatch = "idempotency_mismatch"
	CodeRateLimited         = "rate_limited"
	CodeQuotaExceeded       = "quota_exceeded"
	CodeInternal            = "internal"
	CodeUnavailable         = "unavailable"
)

// Entry is one catalogue row.
type Entry struct {
	Slug   string
	Status int
	Title  string
}

// Catalogue lists every stable problem type.
var Catalogue = []Entry{
	{CodeInvalid, 400, "Invalid Request"},
	{CodeUnauthorized, 401, "Unauthorized"},
	{CodeForbidden, 403, "Forbidden"},
	{CodeNotFound, 404, "Not Found"},
	{CodeConflict, 409, "Conflict"},
	{CodeIdempotencyMismatch, 422, "Idempotency Key Reused"},
	{CodeRateLimited, 429, "Too Many Requests"},
	{CodeQuotaExceeded, 429, "Quota Exceeded"},
	{CodeInternal, 500, "Internal Server Error"},
	{CodeUnavailable, 503, "Service Unavailable"},
}

// Type returns the full type URI for a slug.
func Type(slug string) string { return TypeBase + slug }
