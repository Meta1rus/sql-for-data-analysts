from pydantic import BaseModel


class PostgresConfig(BaseModel):
    host: str
    port: int
    dbname: str
    user: str
    password: str
    driver: str = "psycopg"

    def build_url(self) -> str:
        return (
            f"postgresql+{self.driver}://"
            f"{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

    def build_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"


class ClickhouseConfig(BaseModel):
    host: str
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False
    connect_timeout: int = 10

    def build_jdbc_url(self, database: str | None = None) -> str:
        db = database or self.database
        protocol = "https" if self.secure else "http"
        return f"jdbc:clickhouse:{protocol}://{self.host}:{self.port}/{db}"


class MongoConfig(BaseModel):
    host: str
    port: int = 27017
    username: str | None = None
    password: str | None = None
    database: str = "shop"
    auth_source: str = "admin"
    direct_connection: bool = True

    def build_uri(self) -> str:
        if self.username and self.password:
            credentials = f"{self.username}:{self.password}@"
        else:
            credentials = ""

        uri = f"mongodb://{credentials}{self.host}:{self.port}/"

        params = {
            "authSource": self.auth_source,
            "directConnection": str(self.direct_connection).lower(),
        }

        return uri + "?" + "&".join(f"{key}={value}" for key, value in params.items())


class RedisConfig(BaseModel):
    host: str
    port: int = 6379
    db: int = 0
    username: str | None = None
    password: str | None = None
    ssl: bool = False
    socket_timeout: int = 10
    socket_connect_timeout: int = 10


class SparkConfig(BaseModel):
    app_name: str = "dwh-etl-spark"
    master: str = "local[*]"
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    shuffle_partitions: int = 8
    packages: list[str] = [
        "org.postgresql:postgresql:42.7.4",
        "com.clickhouse:clickhouse-jdbc:0.6.4:all",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
        "com.redislabs:spark-redis_2.12:3.1.0",
    ]
