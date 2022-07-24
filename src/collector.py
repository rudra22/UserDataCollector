from time import sleep
import yaml
import requests
import snowflake.connector
import json


class Collector:
    def __init__(self, config_file="../config/collector.yml"):
        self._config_reader(config_file)
        self._process_on = True
        self._stream_data()

    def _config_reader(self, config_file):
        try:
            with open(config_file, 'r') as stream:
                self._config = yaml.safe_load(stream)
            self._bulk_load = self._config['collector']['bulk_load']
        except Exception as e:
            print(f"Unable to parse config file. Error {repr(e)}")
            exit(1)

    def _stream_data(self):
        url = self._config['collector']['src']['url']
        records_to_fetch = self._config['collector']['src']['records_to_fetch']
        records_fetched = 0

        self._data = []

        while self._process_on and records_fetched < records_to_fetch:
            data = requests.get(url).json()
            if not self._bulk_load:
                self._load_data(data=data)
            else:
                self._data.append(data)
            records_fetched += 1
            sleep(1)

        if self._bulk_load:
            self._load_data()

        print(self._data)

    def _load_data(self, data=None):
        if data or len(self._data) > 0:
            db_type = self._config['collector']['target']['db_type']
            if db_type.lower() == 'snowflake':
                connector = self._snowflake_connector()
                if data:
                    cs = connector.cursor()
                    try:
                        print(json.dumps(data))
                        cs.execute("create table if not exists raw_user_json (raw_json "
                                   "variant);")
                        cs.execute("insert into raw_user_json (select PARSE_JSON('%s'))" % json.dumps(data))
                    except Exception as e:
                        print(f"Error while loading data. Error: {repr(e)}")
                    finally:
                        cs.close()
                    connector.close()
                else:
                    pass
                    # cs = connector.cursor()
                    # try:
                    #     print(json.dumps(data))
                    #     cs.execute("create table if not exists raw_user_json (raw_json "
                    #                "variant);")
                    #     cs.execute("insert into raw_user_json (select PARSE_JSON('%s'))" % json.dumps(self._data))
                    # except Exception as e:
                    #     print(f"Error while loading data. Error: {repr(e)}")
                    # finally:
                    #     cs.close()
                    # connector.close()

    def _snowflake_connector(self):
        connector = snowflake.connector.connect(
            user=self._config['collector']['target']['user'],
            password=self._config['collector']['target']['password'],
            account=self._config['collector']['target']['account'],
            database=self._config['collector']['target']['database'],
            schema=self._config['collector']['target']['schema']
        )
        return connector

    def stop_process(self):
        self._process_on = False


c = Collector()
# c.stop_process()
