#read sql Server(pyodbc)
class MSSQLHandeler:
    import pyodbc
    import pandas as pd
    def __init__(self, DriverClient = None, DbHost = None, database = None, username = None, password = None):
        try:            
            self.result = None
            self.params = "DRIVER={" + DriverClient + "}; SERVER=" + DbHost + "; DATABASE=" + database + "; UID=" + username + "; PWD=" + password
            self.connection = type(self).pyodbc.connect(self.params)
        except Exception as ex:
            print(str(ex))
    def GetDF(self, sql = None):
        try:
            rs = type(self).pd.read_sql_query(sql, self.connection)
            self.result = rs
            self.connection.close()
        except Exception as ex:
            return ex;
        finally:
            return self.result
    def Comand(self, sql = None):
        try:
            #rs = self.connection.execute(sql).fetchall()
            rs = type(self).pd.read_sql_query(sql, self.connection)
            self.result = rs
            self.connection.close()
        except Exception as ex:
            return ex;
        finally:
            return self.result
