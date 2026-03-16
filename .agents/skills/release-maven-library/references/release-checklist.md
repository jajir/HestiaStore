# Release Checklist

- Verify the working tree is clean before changing versions.
- Run `mvn clean verify`.
- Confirm the intended release version.
- Confirm there are no forbidden snapshot dependencies or plugins.
- Tag the release after the release version is verified.
- Bump the project to the next snapshot version.
- Verify again after the next snapshot bump.
