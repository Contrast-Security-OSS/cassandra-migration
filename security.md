# Security Policy

Contrast Security is committed to the security of our users and our open-source software. We appreciate the efforts of security researchers who help us keep our products safe.

## Supported Versions

We actively support and provide security updates for the following versions of our projects. If you are using a version not listed below, please upgrade to a supported version.

| Version | Supported |
| ------- | ------------------ |
| < 1.0.0   | ✅ Supported     |
| 0.x.x   | ❌ Not Supported   |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

We offer two ways to report a vulnerability:

### 1. Private Vulnerability Reporting (Preferred)
The easiest way to report a vulnerability is via GitHub's [Private Vulnerability Reporting](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/privately-reporting-a-security-vulnerability) feature. Navigate to the **"Security"** tab of the specific repository and click **"Report a vulnerability"**.

### 2. Email
Alternatively, you can email your report to [security@contrastsecurity.com](mailto:security@contrastsecurity.com). 

For more details on our processes, please see our official [Vulnerability Disclosure Policy](https://www.contrastsecurity.com/disclosure-policy).

### What to include in your report:
* A description of the vulnerability and its potential impact.
* A clear, step-by-step guide to reproducing the issue (PoC scripts or screenshots are helpful).
* The version of the software affected.

## Our Response Process

Contrast takes every report seriously. After you submit a report:

* **Acknowledgment:** We will acknowledge receipt of your report within 2 business days.
* **Investigation:** Our security team will investigate the report and may reach out for more information.
* **Updates:** We will keep you informed of our progress as we work toward a fix.
* **Disclosure:** We follow coordinated disclosure. We ask that you do not share the vulnerability publicly until we have released a fix and an official announcement.

## Policy on Dependency Updates

To ensure the stability of our ecosystem, we follow a **7-day "soak" period** for most dependency updates. This allows the community to identify any "left-of-vulnerability" issues or regressions in new upstream releases before we integrate them.

If you have specific concerns regarding a high-severity CVE in one of our dependencies, please contact us at the email above.

## Third-Party Modules

Reports regarding security bugs in third-party modules should be directed to the person or team maintaining that specific module. However, if a third-party vulnerability creates a direct risk to a Contrast project, please let us know.

## Learning More

To learn more about securing your applications with Contrast, please visit [our documentation](https://docs.contrastsecurity.com/?lang=en).
