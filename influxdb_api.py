import os
import getpass
import pickle
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError


class DB:
    def __init__(self, host='192.168.1.7', port=8086):
        username, password = self.login()
        config = {'host': host, 'port': port, 'username': username, 'password': password}
        self.client = InfluxDBClient(**config)

    def login(self):
        username = input('username :')
        try:
            password = os.environ['INFLUXDB_PASSWORD1']
        except KeyError:
            password = getpass.getpass('password :')

        self.username = username
        return username, password

    '''
    IO
    '''
    # NaN 해결
    def write_db(self, df, tags, fields, time='time'):
        database = input('database :')
        try:
            self.__check_if_including_database(database)
        except InfluxDBClientError:
            pass

        self.client.switch_database(database)

        add_string = ''
        while True:
            string = '\rmeasurement :' + add_string
            measurement = input(string)
            query = self.client.query('SHOW MEASUREMENTS')
            if len(query) != 0:
                measurement_list = query.raw['series'][0]['values']
                measurement_list = [measure[0] for measure in measurement_list]
                add_string = f'{measurement} is duplicated'
                if measurement not in measurement_list:
                    break
            else:
                break

        ar = df.values
        columns = df.columns
        tags_loc = [col in tags for col in columns]
        fields_loc = [col in fields for col in columns]
        time_loc = [col == time for col in columns]

        tag_columns = columns[tags_loc]
        field_columns = columns[fields_loc]

        ar_tags = ar[:, tags_loc]
        ar_fields = ar[:, fields_loc]
        ar_time = ar[:, time_loc]

        points = []
        for ar_t, ar_f, t in zip(tqdm(ar_tags), ar_fields, ar_time):
            tags_dict = {key: value for key, value in zip(tag_columns, ar_t)}
            fields_dict = {key: value for key, value in zip(field_columns, ar_f)}

            point = {
                "measurement": measurement,
                "time": int(t[0].timestamp() * 1e9),
                "fields": fields_dict,
                "tags": tags_dict,
            }
            points.append(point)

            if len(points) == 10000:
                response = self.client.write_points(points)
                points = []

        response = self.client.write_points(points)

    def read_db(self, save_df=True, save_final_pkl=True):
        database = input('database :')
        self.__check_if_including_database(database)
        self.client.switch_database(database)

        measurement_list = self.client.get_list_measurements()
        measurement_list = [measure['name'] for measure in measurement_list]
        string = 'Please enter the number of the measurement you want to read\n'
        for num, measure in enumerate(measurement_list):
            string += f'{num}: {measure}\n'

        measurement_num = input(string)
        measurement = measurement_list[int(measurement_num)]
        print(measurement)

        result = self.client.query(f'select * from "{measurement}"')

        points = result.get_points()

        row_list = []
        for a in points:
            row_list.append(a.values())
        df = pd.DataFrame(row_list, columns=a.keys())

        if save_df:
            os.makedirs('data', exist_ok=True)
            df.to_feather(f'data/{database}_{measurement}.ftr')

        if save_final_pkl:
            final = list(df.groupby(tags))
            os.makedirs('data', exist_ok=True)
            with open(f'data/{database}_{measurement}.pkl', 'wb') as f:
                pickle.dump(final, f)

        return df

    '''
    create
    '''

    def create_database(self):
        database = input('database :')
        self.client.create_database(database)

    def create_user(self):
        username = input('username :')
        password = getpass.getpass('password :')
        self.client.create_user(username, password, False)

    '''
    drop
    '''

    def drop_measurement(self):
        database = input('database :')
        self.client.switch_database(database)

        measurement_list = self.client.get_list_measurements()
        measurement_list = [measure['name'] for measure in measurement_list]

        string = 'Please enter the number of the measurement you want to delete\n'
        for num, measure in enumerate(measurement_list):
            string += f'{num}: {measure}\n'

        measurement_num = input(string)
        measurement = measurement_list[int(measurement_num)]

        if self.__drop_double_check(measurement):
            self.client.drop_measurement(measurement)

    def drop_user(self):
        username = input('username :')
        self.__check_if_including_username(username)
        if self.__drop_double_check(username):
            self.client.drop_user(username)

    def drop_database(self):
        database = input('database :')
        self.__check_if_including_database(database)
        if self.__drop_double_check(database):
            self.client.drop_database(database)

    def __drop_double_check(self, name):
        answer = input(f'Do you really want to drop {name}? (y/N)')
        return answer == 'y'

    '''
    others
    '''

    def change_password(self):
        username = input('username :')
        password = getpass.getpass('password :')
        self.client.set_user_password(username, password)

    def grant(self):
        while True:
            privilege = input("privilege\n1. 'all'\n2. read\n3. write")
            if privilege in ['1', '2', '3']:
                privilege = ['all', 'read', 'write'][int(privilege) - 1]
                break
            else:
                continue
        username = input('username :')
        self.__check_if_including_username(username)
        database = input('database :')
        self.__check_if_including_database(database)

        self.client.grant_privilege(privilege, database, username)

    def get_auth_sheet(self):
        username_list = self.client.get_list_users()
        username_list = [dict_['user'] for dict_ in username_list]

        database_list = self.client.get_list_database()
        database_list = [dict_['name'] for dict_ in database_list]

        print(f'user list\n{username_list}')
        print(f'database list : {database_list}')

        while True:
            username = input('break key: -1\nusername :')
            if username == '-1':
                break
            try:
                print(f'{username} :', self.client.get_list_privileges(username))
            except:
                pass

    def __check_if_including_username(self, username):
        username_list = self.client.get_list_users()
        username_list = [dict_['user'] for dict_ in username_list]
        if username not in username_list:
            raise Exception(f'influxDB does not contain a {username}.')

    def __check_if_including_database(self, database):
        database_list = self.client.get_list_database()
        database_list = [dict_['name'] for dict_ in database_list]
        if database not in database_list:
            raise Exception(f'influxDB does not contain a {database}.')

    def get_measurement_list(self):
        database = input('database :')
        try:
            self.__check_if_including_database(database)
        except InfluxDBClientError:
            pass
        self.client.switch_database(database)
        measurement_list = self.client.query('SHOW MEASUREMENTS').raw['series'][0]['values']
        measurement_list = [measure[0] for measure in measurement_list]
        print(measurement_list)
