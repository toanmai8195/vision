# iceberg-rest-fixture + driver Postgres: catalog dev lưu metadata ở Postgres (nhiều sink commit song song, SQLite thì không chịu nổi).
# Image gốc chỉ có driver SQLite nên thêm jar và chạy lại server với classpath mở rộng (java -jar bỏ qua classpath).
FROM apache/iceberg-rest-fixture:1.9.2

ADD --chown=iceberg:iceberg --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /home/iceberg/postgresql.jar

CMD ["java", "-cp", "iceberg-rest-adapter.jar:/home/iceberg/postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
