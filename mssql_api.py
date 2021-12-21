#read sql Server(pyodbc)
class MSSQLHandeler:
    def __init__(self, DriverClient = None, DbHost = None, database = None, username = None, password = None):
        try:
            import pyodbc
            self.result = None
            self.params = "DRIVER={" + DriverClient + "}; SERVER=" + DbHost + "; DATABASE=" + database + "; UID=" + username + "; PWD=" + password
            self.connection = pyodbc.connect(self.params)
        except Exception as ex:
            print(str(ex))
    def GetDF(self, sql = None):
        try:
            import pandas as pd
            rs = pd.read_sql_query(sql, self.connection)
            self.result = rs
        except Exception as ex:
            return ex;
        finally:
            return self.result
    def Comand(self, sql = None):
        try:
            import pandas as pd
            #rs = self.connection.execute(sql).fetchall()
            rs = pd.read_sql_query(sql, self.connection)
            self.result = rs
        except Exception as ex:
            return ex;
        finally:
            return self.result