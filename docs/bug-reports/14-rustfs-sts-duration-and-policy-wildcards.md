# [RustFS] STS: DurationSeconds below 900 silently raised to 900; wildcard actions in session policies refused

**Component:** RustFS STS (AssumeRole)
**Version:** RustFS 1.0.6

## Summary

1. `AssumeRole` with `DurationSeconds=60` succeeds and returns keys that expire after 900 s. AWS
   STS refuses values below its minimum (900) with 400 ValidationError. A client that asks for
   short-lived keys (e.g. a catalog vending keys for a single query) silently gets keys that live
   15 times longer.
2. A session policy with a wildcard in an action (`"Action": ["s3:Get*"]`) is refused:
   `InvalidRequest: Failed to parse session policy: ... invalid action: 's3:Get*'`. AWS IAM and STS
   allow wildcards in actions; Unity Catalog's vended-credential session policies use them (the
   lab has to expand them in a proxy before the request reaches RustFS).

## Steps to reproduce

```python
sts = boto3.client("sts", endpoint_url=RUSTFS, ..., config=Config(parameter_validation=False))
sts.assume_role(RoleArn="arn:aws:iam::123456789012:role/lab", RoleSessionName="lab",
                DurationSeconds=60, Policy=json.dumps(POLICY))   # 1: Expiration = now + 900 s
POLICY = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:Get*"],
          "Resource": ["arn:aws:s3:::bucket/prefix/*"]}]}         # 2: InvalidRequest
```

## Expected

1. 400 (as AWS) - or at least keys that expire after the requested 60 s.
2. The wildcard covers `s3:GetObject` etc., as in AWS.

## Notes

- Lab tests: `tests/storage/test_storage_s3.py::test_sts_session_policy[short_duration|wildcard_action]`.
- Prefix-scoped session policies and Deny statements are enforced correctly (same test file).
