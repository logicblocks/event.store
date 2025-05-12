from logicblocks.event.persistence.postgres import ConnectionSettings


class TestPostgresConnectionSettings:
    def test_includes_all_settings_in_representation_obscuring_password(self):
        settings = ConnectionSettings(
            host="localhost",
            port=5432,
            dbname="event_store",
            user="user",
            password="supersecret",
        )

        assert repr(settings) == (
            "ConnectionSettings("
            "host=localhost, "
            "port=5432, "
            "dbname=event_store, "
            "user=user, "
            "password=***********"
            ")"
        )

    def test_generates_connection_string_from_parameters(self):
        settings = ConnectionSettings(
            host="localhost",
            port=5432,
            dbname="event_store",
            user="user",
            password="supersecret",
        )

        assert (
            settings.to_connection_string()
            == "postgresql://user:supersecret@localhost:5432/event_store"
        )
