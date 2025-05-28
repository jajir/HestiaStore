# Documentation

Documentation is available in the following locations:

* **`mvn site`** - Includes detailed project reports from PMC, checkstyle, JUnit test line coverage, and Javadocs. It's not published.
* **[GitHub project](https://github.com/jajir/HestiaStore/)** - Simple technical development oriented site
* **[HestiaStore.org site](https://hestiastore.org)** - Detailed, user-focused information

Following text is about HestiaStore.org site documentation.

## How to make changes to HestiaStore.org

HestiaStore.org site documentation is served from the main project branch `gh-pages`. Publishing a new site version involves generating HTML from Markdown and pushing it to `gh-pages`.

Prerequisites:

* Installed git
* Site generating tool mkdocs-material - as described at [https://squidfunk.github.io/mkdocs-material/](https://squidfunk.github.io/mkdocs-material/). It is user-friendly and easy to work with. In case of MacOS install it with:

```bash
brew install mkdocs-material
```
* Some Markdown editor of your choice
* GitHub personal access token with permission to read and write project pages.

### Page editing and viewing documentation locally

From project checkout branch `docs`, there are all source files for main site. Markdown files for documentation are located in the directory `docs`. To preview documentation changes locally, run:

```bash
mkdocs serve
```

Now at [http://127.0.0.1:8080/HestiaStore/](http://127.0.0.1:8080/HestiaStore/) should display the documentation.

The `mkdocs.yml` file in the root directory controls site structure, navigation, and theme. For more information see mkdocs-material documentation.

### How to publish changes at hestiastore.org

* From [github.com/jajir/HestiaStore](https://github.com/jajir/HestiaStore/) checkout branch `docs`. 
* Make changes
* Commit changes to `docs`
* Then, run the following command locally:

  ```bash
  mkdocs gh-deploy
  ```
  In a few minutes (could be 15 minutes) new documentation will be published.