# -*- coding: utf-8 -*-
import psycopg2
#sys.path.append(‘/Volumes/Data/Documents/GitHub/Follow-up-trigger’) 

'''
1.接收导星和调焦动作信息：在接受到信息后，入库
2,发送导星信息：每5秒钟查询一次数据库（update select），进行判断，如适合则发送到客户端。
'''

#nohup python getOTImgsAll.py > /dev/null 2>&1 &
class GWACQuery:
    
    webServerIP1 = '172.28.8.28:8080'
    webServerIP2 = '10.0.10.236:9995'
    
    connParam={
        "host": "190.168.1.27",
        "port": "5432",
        "dbname": "gwac2",
        "user": "gwac",
        "password": "gdb%980"
        }
    # 2 xinglong server
    connParam2={
        "host": "172.28.8.28",
        "port": "5432",
        "dbname": "gwac2",
        "user": "gwac",
        "password": "gdb%980"
        }
    connParam3={
        "host": "10.0.3.62",
        "port": "5433",
        "dbname": "gwac2",
        "user": "gwac",
        "password": "gdb%980"
        }
    # 4 beijing sever
    connParam4={
        "host": "10.0.10.236",
        "port": "5432",
        "dbname": "gwac2",
        "user": "gwac",
        "password": "gdb%980"
        }
    
    fwhmQuery = "SELECT " \
        "from " \
        "update" 
    
    focusCommend = "insert "
    guideCommend = "insert "
    
    dirHRDImage = "/home/gwac/software/"
    #dirHRDImage = "/Volumes/Data/Documents/GitHub/Follow-up-trigger"
    
    def __init__(self):
        
        self.conn = False
        self.verbose = False
        
    def connDb(self):
        
        self.conn = psycopg2.connect(**self.connParam4)
        self.dataVersion = ()
        
    def closeDb(self):
        self.conn.commit()
        self.conn.close()
            
    def getDataFromDB(self, sql):
                
        tsql = sql
        
        try:
            self.connDb()
    
            cur = self.conn.cursor()
            cur.execute(tsql)
            rows = cur.fetchall()
            cur.close()
            self.closeDb()
        except Exception as err:
            rows = []
            print("error: getDataFromDB query data error")
            # self.log.error(" query data error ")
            # self.log.error(err)
            
        return rows

    def insertFocusAction(self, ot2Name):
    
        tsql = "update science_object set status=1, trigger_status=1, auto_observation=true where name='%s'"%(ot2Name)
        #self.log.debug(tsql)
        
        try:
            self.connDb()
    
            cur = self.conn.cursor()
            cur.execute(tsql)
            self.conn.commit()
            cur.close()
            self.closeDb()
        except Exception as err:
            # self.log.error(" init science_object status error ")
            print(err)

    def insertGuideAction(self, ot2Name):
        pass

    def queryFwhmRecords(self):
        
        tsql = "select mnt.unit_id, mnt.group_id, cam.name camname, CURRENT_TIMESTAMP -time_obs_ut-INTERVAL '8 HOUR', "\
            "to_char(time_obs_ut, 'YYYY-MM-DD HH24:MI:SS'), fwhm, astro_flag, obj_num, bg_bright, s2n, avg_limit "\
            "from image_status_parameter isp "\
            "INNER JOIN camera cam on cam.camera_id=isp.dpm_id "\
            "INNER JOIN mount mnt on mnt.mount_id=cam.mount_id "\
            "WHERE astro_flag>=-2 and astro_flag<=1 and CURRENT_TIMESTAMP -time_obs_ut-INTERVAL '8 HOUR' < INTERVAL '30 SECOND' "\
            "ORDER BY time_obs_ut desc, camname "\
            "limit 10;"
        #self.log.debug(tsql)
        
        tresult = self.getDataFromDB(tsql)
        return tresult
    
    def queryFwhmRecordsFromUpdate(self):
        
        tsql = "with updated_rows as "\
            "(update image_status_parameter as isp  "\
            "set send_success=true  "\
            "from camera as cam, mount as mnt "\
            "where cam.camera_id=isp.dpm_id and mnt.mount_id=cam.mount_id  "\
            "    and astro_flag>=-2 and astro_flag<=1 and send_success=false  "\
            "    and CURRENT_TIMESTAMP -time_obs_ut-INTERVAL '8 HOUR' < INTERVAL '30 SECOND' "\
            "returning  "\
            "    mnt.unit_id, mnt.group_id, cam.name camname, "\
            "    to_char(time_obs_ut, 'YYYY-MM-DD HH24:MI:SS'),  "\
            "    fwhm, astro_flag, obj_num, bg_bright, s2n, avg_limit,isp_id)  "\
            "select fucosInfo.*  "\
            "from updated_rows fucosInfo order by isp_id"
        # print(tsql)
        
        tresult = self.getDataFromDB(tsql)
        # for trow in tresult:
        #     print(trow[0], trow[1], trow[4], trow[5])
        return tresult
               
    def test(self):
        try:
            tresult = self.queryFwhmRecordsFromUpdate()
            print(tresult)
        except Exception as err:
            print(err)
            
if __name__ == '__main__':
    
    gwacQuery = GWACQuery()
    gwacQuery.test()
    

