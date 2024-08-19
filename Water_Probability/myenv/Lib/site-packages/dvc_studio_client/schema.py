from voluptuous import All, Any, Exclusive, Lower, Match, Optional, Required, Schema


def choices(*choices):
    """Checks that value belongs to the specified set of values"""
    return Any(*choices, msg=f"expected one of {', '.join(choices)}")


Sha = All(Lower, Match(r"^[0-9a-h]{40}$"), msg="expected a length 40 commit sha")
# Supposed to be produced with:
#     {"cls": f"{e.__class__.__module__}.{e.__class__.__name__}", "text": str(e)}
ERROR_SCHEMA = {Required("cls"): str, Required("text"): str}

BASE_SCHEMA = Schema(
    {
        Required("type"): choices("start", "done", "data"),  # No "interrupt" for now
        Required(
            "repo_url",
        ): str,  # TODO: use some url validator, voluptuous.Url is too strict
        Required("baseline_sha"): Sha,
        "name": str,
        "env": dict,
        "client": str,
        "errors": [ERROR_SCHEMA],
        "params": {str: dict},
        "metrics": {str: {"data": dict, "error": ERROR_SCHEMA}},
        "machine": dict,
        # Required("timestamp"): iso_datetime,  # TODO: decide if we need this
    },
)
SCHEMAS_BY_TYPE = {
    "start": BASE_SCHEMA.extend(
        {
            "message": str,
            Optional("subdir"): str,
        },
    ),
    "data": BASE_SCHEMA.extend(
        {
            Required("step"): int,
            "plots": {
                str: {
                    Exclusive("data", "data"): [dict],
                    "props": dict,
                    "error": ERROR_SCHEMA,
                    Exclusive("image", "data"): str,
                },
            },
        },
    ),
    "done": BASE_SCHEMA.extend(
        {
            "experiment_rev": Sha,
        },
    ),
}
