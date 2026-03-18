# Release Checklist

- Verify the working tree is clean before changing versions.
- Confirm the current Maven version and whether it is a `-SNAPSHOT`.
- Confirm `~/.m2/settings.xml` has the `central` server credentials and `release` profile GPG settings.
- Confirm a usable GPG secret key exists locally.
- Run `mvn clean verify`.
- Confirm the intended release version.
- Confirm there are no forbidden snapshot dependencies or plugins.
- Confirm the release deploy includes the parent POM and does not deploy `engine` alone.
- Run `mvn -pl .,:engine -P release -DskipTests verify` before deployment.
- Tag the release after the release version is verified.
- Deploy to Sonatype Central and watch for `PUBLISHING` or `PUBLISHED`.
- Bump the project to the next snapshot version.
- Verify again after the next snapshot bump.
