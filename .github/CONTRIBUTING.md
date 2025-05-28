# Contributing to HestiaStore

Thank you for your interest in contributing to HestiaStore! We're excited to welcome your ideas, improvements, and bug fixes. Please follow the guidelines below to ensure a smooth and productive collaboration.

## 🧭 Before You Start

Please make sure there is an existing issue or create a new one that describes your intended change or feature. This helps us track and discuss proposals before any code is written.

## 🧑‍💻 Code Style

We follow a consistent Java coding style defined by the Eclipse formatter settings in [`./eclipse-formatter.xml`](./eclipse-formatter.xml). Please configure your IDE to use this formatter to keep the codebase consistent.

## 🧪 Code Quality Checks

Before submitting your code, please verify the following:

- ✅ **Run Site Checks**  
  Execute `mvn clean site` to generate the project site and perform static analysis. This will highlight issues reported by:
  - PMD
  - Checkstyle
  - SpotBugs (formerly FindBugs)  
  Please ensure your changes do not introduce new warnings or violations.

- ✅ **Test Coverage**  
  All new code should be covered by unit tests. We use JUnit. Run tests and verify that your code is being exercised by checking the line coverage in the site reports.

- ✅ **Javadoc Comments**  
  Public methods, classes, and significant internal logic should be documented using Javadoc. Clear documentation helps others understand and maintain the project.

## 🛠 Commit and Submit

1. Make your changes in a separate branch.
2. Push your branch to your fork or the main repo (if you have access).
3. Open a **Pull Request** with a clear title and description.
4. Link to the related issue or ticket.

## ⏳ Review Process

Once submitted, your PR will be reviewed by a maintainer. We may request changes or ask clarifying questions. Please be patient — reviews are important to keep the project healthy.

## 🙌 Thanks

We appreciate your contribution, whether it's code, documentation, or ideas. Your support makes HestiaStore better!

---

💬 For questions, feel free to open a GitHub Discussion or contact a maintainer.
