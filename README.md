# jbindex

It's simple fast index. Work with index should be split into phases of:

* Writing data to index. All data that should be stored in index should be send to index.
* Building index. In this phase data are organized for fast access.
* Search through index. In this phase it's not possible to alter data in index.

Index is not thread safe.

## Basic work with index

Index could be in following states:

![Index states](./src/images/index-state-diagram.png)

Interruption of process of writing data to index could lead to corruption of entire index.

## Development 

Mockito requires reflective access to non-public parts in a Java module. It could be manually open by passing following parameter as jvm parameter:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
```
