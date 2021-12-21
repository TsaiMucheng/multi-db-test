class MongoHandeler:
    def __init__(self, mongoDbHost, database, username, password):
        # Method 1 : Local Test  
        from pymongo import MongoClient
    
        mg = MongoClient
        
        try:
            self.mongoDbHost = mongoDbHost
            self.database = database
            self.username = username
            self.password = password
            client = mg(mongoDbHost)
            login = client[database].authenticate(username, password)
        
            if login != True:
                raise Exception('Login failed!!');
                
            self.db = client[database]
        except Exception as ex:
            print(str(ex))
            raise Exception(ex)        
            #return str(ex);

    def __StrToJson(self, __json):
        """
        __json is str type
        rerurn dict, list type
        """
        import json as js
        result_str = None
        result_str = js.loads(__json)
        return result_str
    
    def Tst__StrToJson(self, __json):
        """
        __json is str type
        rerurn dict, list type
        """
        import re, ast
        import json as js
        result_str = None
        try:
            
            p = re.compile('(?<!\\\\)\'')
            __json = p.sub('\"', __json)
            __json = re.sub(r"[“|”|‛|’|‘|`|´|″|′|']", '"', __json)        
            result_str = ast.literal_eval(js.dumps(__json))
            if type(result_str) is not str:
                return result_str
            else:
                result_str = js.loads(result_str)
                if type(result_str) is not str:
                    return result_str
                else:
                    tmp = result_str[0:30]
                    raise Exception('Type is Error while loding this line: \r' + tmp)
        except Exception:
            result_str = js.loads(__json)
            return result_str
    
    #1.1	Create collection
    def CreateCol(self, collection_name, collations):
        Result = False;
        Msg = '';
        try:         
            if (str(type(collations)) != "<class 'dict'>") and (str(type(collations)) != "<class 'pymongo.collation.Collation'>"):
                collations = self.__StrToJson(str(collations))
            #Collation is a new feature in MongoDB version 3.4.
            #Create By Collation()
            colletemp = self.db.create_collection(
                name = collection_name,
                codec_options = None,
                read_preference = None,
                write_concern = None,
                read_concern = None,
                session = None,
                collation = collations)
            Msg = str(colletemp)
            Result = True;
        except Exception as ex:
            # collection already exists
            if "already exists" in str(ex):
                Msg = self.db[collection_name]
                Result = True;
            else:
                Err = "create_collection() ERROR:" + str(ex)
                Result = False;
                Msg = Err;
        finally:
            resultDict = {
              "Result": Result,
              "Msg": Msg
            }
            return resultDict;
        
    #1.2	Drop collection
    def DropCol(self, collection_name):
        Result = False;
        Msg = '';
        try:
            Msg = self.db.drop_collection(collection_name)
            Result = True;
        except Exception as ex:
            Err = "drop_collection() ERROR:" + str(ex)
            Result = False;
            Msg = Err;
        finally:
            resultDict = {
              "Result": Result,
              "Msg": str(Msg)
            }
            return resultDict;
        
    #2.1	Insert
    def Insert(self, collection_name, json, one):
        """
        json is string, dict or list type
        convert to json type in function
        one means insert_one else insert_many
        """
        Result = False;
        Msg = '';
        try:
            json = self.__StrToJson(json) if type(json) is str else json
            Col = self.db.collection
            if collection_name != None and collection_name != '':
                Col = self.db[collection_name]
            
            if (one == True) or (str(type(json)) == "<class 'dict'>"):
                output = Col.insert_one(json)
                Result = True;
                Msg = 'inserted_id: ' + str(output.inserted_id);
            elif (one != True) or (str(type(json)) == "<class 'list'>"):
                json = list(json)
                output = Col.insert_many(json)
                Result = True;
                Msg = 'inserted_id: ' + str(output.inserted_id);
            else:
                Msg = 'Type is ' + str(type(json))
                raise Exception(Msg);
        except Exception as ex:
            Err = "insert_*() ERROR:" + str(ex)
            Result = False;
            Msg = Err;
        finally:
            resultDict = {
              "Result": Result,
              "Msg": str(Msg)
            }
            return resultDict;        

    #2.2	Update
    def Update(self, collection_name, query, newvalue, one, *args):
        """
        collection_name is table name
        query is real value
        newvalue is what we want to change to be
        one means update_one else update_many
        
        *args
        upsert means insert if not exists
        """
        Result = False;
        Msg = '';
        import json as js
        
        #*args defination
        upsert = args[0] if len(args) != 0 else True
        
        #upsert
        try:
            query = self.__StrToJson(query) if type(query) is str else query
            newvalue = str(newvalue) if type(newvalue) is not str else newvalue
            setnewvalue = '{"$set":' + js.dumps(newvalue) + '}'
            setnewvalue = self.__StrToJson(setnewvalue)
            Col = self.db.collection
            if collection_name != None and collection_name != '':
                Col = self.db[collection_name]                
            if (one == True) or (type(query) is dict):
                if type(setnewvalue) is str:
                    tmp = setnewvalue[:30]
                    print(tmp)
                    setnewvalue = self.__StrToJson(setnewvalue)
                    tmp = setnewvalue[:30]
                    print(tmp)
                
                Col.update_one(query, setnewvalue, upsert = upsert)
                resultSet = self.Find(collection_name, newvalue)              
                Result = True;
                Msg = str(resultSet.get("Msg"));
            elif (one != True) or (type(query) is list):
                print('type(query) is list')
                #output = 
                Col.update_many(query, setnewvalue, upsert)
                resultSet = self.Find(collection_name, newvalue)
                Result = True;
                Msg = str(resultSet.get("Msg"));
            else:
                Msg = 'Type is ' + str(type(query))
                raise Exception(Msg);
        except Exception as ex:
            Err = "update_*() ERROR:" + str(ex)
            Result = False;
            Msg = Err;
        finally:
            resultDict = {
              "Result": Result,
              "Msg": str(Msg)
            }
            return resultDict;
    def Delete(self, collection_name, json, one):
        Result = False;
        Msg = '';
        import json as js
        try:
            json = self.__StrToJson(json) if type(json) is str else json                
            Col = self.db.collection
            if collection_name != None and collection_name != '':
                Col = self.db[collection_name]
            
            if (one == True) or (str(type(json)) == "<class 'dict'>"):
                output = Col.delete_one(json)
                Result = True;
                Msg = 'deleted_count: ' + str(output.deleted_count);
            elif (one == True) or (str(type(json)) == "<class 'list'>"):
                json = list(json)
                output = Col.delete_many(json)
                Result = True;
                Msg = 'deleted_count: ' + str(output.deleted_count);
            else:
                Msg = "Type is " + str(type(json));
                raise Exception(Msg)
        except Exception as ex:
            Err = "delete_*() ERROR:" + str(ex)
            Result = False;
            Msg = Err;
        finally:
            resultDict = {
              "Result": Result,
              "Msg": str(Msg)
            }
            return resultDict;
        
    #4.0	Get All collection Name From DB
    def List(self, flt = {"name": {"$regex": r"^(?!system\.)"}}):
        try:
            result = self.db.list_collection_names(filter = flt)
            return result
        except Exception as ex:
            Err = "list_collection_names() ERROR:" + str(ex)
            return Err;
         
    #4.1	Query
    def Find(self, collection_name, json, *args):#, flt = {"name": {"$regex": r"^(?!system\.)"}}
        """
        collection_name is table name
        flt is dict()
        json is dict()
        return True if msg is dict
            msg including query dict from mongo DB
        return False if msg is string
        """
        #*args defination
        flt = args[0] if len(args) != 0 else None
        
        run_find = lambda table, __flt, __json: list(self.db[table].find(__flt, __json)) if __flt != None else list(self.db[table].find(__json))
        
        import json as js
        json = json or {}
        Result = False;
        Msg = '';
        try:
            json = self.__StrToJson(json) if type(json) is str else json
            flt = self.__StrToJson(flt) if type(flt) is str else flt
            if json == None and (collection_name != None and collection_name != ''):
                result = run_find(collection_name, flt, json)
                resultSet = dict()
                for x in result:
                    resultSet.update(x)
                Msg = resultSet
                Result = True;
            elif json == None and (collection_name == None or collection_name == ''):
                Msg = 'No input! collection_name: Table Name; json: Query Column and Value;'
                Result = True;
            elif json != None and (collection_name == None or collection_name == ''):
                cols = self.db.list_collection_names()                
                for col in cols:
                    result = run_find(str(col), flt, json)
                resultSet = dict()
                for x in result:
                    resultSet.update(x)
                Msg = resultSet
                Result = True;
            else:
                result = run_find(collection_name,flt,json)
                resultSet = dict()
                for x in result:
                    resultSet.update(x)
                Msg = resultSet
                Result = True;
        except Exception as ex:
            Err = "find() ERROR:" + str(ex)
            Result = False;
            Msg = Err;
        finally:
            resultDict = {
              "Result": Result,
              "Msg": Msg
            }
            return resultDict;
        
    #reference.	Collation
    #ex. colla = {'locale': 'en_US', 'strength': 2, 'numericOrdering': True, 'backwards': False}
    def Collation(
            self, 
            locale = 'en_US', 
            strength = 2, 
            numericOrdering = True, 
            backwards = False):
        from pymongo.collation import Collation
        Result = False;
        Msg = '';
        try:
            colla = Collation(
                    locale = locale,
                    strength = strength,
                    numericOrdering = numericOrdering,
                    backwards = backwards)
            Result = True
            return colla;
        except Exception as ex:
            Result = False;
            Msg = "Collation() ERROR:" + str(ex)
            resultDict = {
              "Result": Result,
              "Msg": str(Msg)
            }
            return resultDict;