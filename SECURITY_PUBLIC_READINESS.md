# Security Public-Readiness

## Branch Protection (GitHub Settings)
Set these on `main` before making the repository public:
- Require pull request before merge.
- Require status checks to pass.
- Require `Secret Scan / Gitleaks`.
- Restrict force pushes (allow admins only, temporary exceptions for history rewrite only).

## Local Pre-Commit Hook
Enable repository-managed hooks:

```bash
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
```

## After History Rewrite
If history was force-pushed, collaborators must re-clone or hard-reset local clones to new `origin/main`.
