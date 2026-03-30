### ✨ What’s Changed?

<!-- Describe what this PR changes and why -->

### 🔍 How to Review / Test

<!-- Include commands, curl examples, and/or how to validate in staging -->

### 📋 Checklist

- [ ] PR title starts with `[#issue-number]`; if no issue is applicable use: `[fix]`, `[feat]`, `[ops]`, or `[prod]`
- [ ] I’ve added/updated tests (or noted why they’re not needed)
- [ ] I’ve verified behavior against a local Redis (and pipeline workers if relevant)
- [ ] If this impacts queues/processes/pipeline config, I’ve validated it won’t break existing runs
- [ ] If this changes auth (JWT) behavior, I’ve verified protected vs public endpoints
- [ ] If this changes S3 upload/presigning, I’ve verified required `S3_*` env vars and error handling
- [ ] I’ve considered observability (logs/metrics/traces) and added what’s needed
- [ ] I’ve set labels, linked issue, and milestone (if applicable)

### 🛠 Related Issue

Closes #[issue-number] <!-- or: Related to #[issue-number] -->
