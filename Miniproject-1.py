import yaml
import logging
import cx_Oracle as cx
'''
mini project for extracting the data from sql and put it in notepad file
copyright lks jan 2021
'''
logging.basicConfig(filename='project.log',level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

class Project():
    def __init__(self):
        self.companyname=""
        self.address=""
        self.username=""
        self.password=""
        self.host=""
        self.final=""
        self.result=""

    def load_config(self):
        try:
            with open(r'projectconfig') as config:
                config_list=yaml.load(config,Loader=yaml.FullLoader)
                logging.info("successfully created the config file")
                self.companyname=config_list['companyname']
                self.address=config_list['address']
                self.username=config_list['username']
                self.password=config_list['password']
                self.host=config_list['host']
                self.result=config_list['result']
                logging.info("successfully read the data from the yaml file")
        except Exception as e:
            logging.error("failed to read the data")

    def sqlconnect(self):
        try:
            con = cx.connect(self.username+"/"+self.password+"@"+self.host)
            logging.info("successfully connected to database")
            cursorobj = con.cursor()
            try:
                record=cursorobj.execute('SELECT * FROM emp')
                for result in record:
                    print(result)
                print("\n")

                total_count_emp = cursorobj.execute('select count(empno) from emp')
                countemp = self.count_emp(total_count_emp)
                print(countemp)
                print("\n")

                total_sum_emp= cursorobj.execute('select sum(sal) from emp')
                totalsum=self.totalsumemp(total_sum_emp)
                print(totalsum)
                print("\n")

                temp_query_var= ['select sum(sal),job from emp group by job']
                job_3=cursorobj.execute(temp_query_var[0])
                Sum_of_individuals=self.Sumofindividual(job_3)
                print(Sum_of_individuals)
                print("\n")
                self.final="COMPANY NAME:"+self.companyname+"\n"+"COMPANY ADDRESS:"+self.address+"\n"+\
                           countemp+"\n"+totalsum+"\n"+"\n"+"SUM OF INDIVIDUALs:"+"\n"+Sum_of_individuals
                print(self.final)
                with open(self.result, 'w') as file:
                    file.write(self.final)
                    logging.info("successfully copied to output file")

            except Exception as e:
                    logging.error("Execution failed due to invalid query")

            con.commit()
            cursorobj.close()
            con.close()

        except cx.DatabaseError as e:
            logging.error("exception occurred while trying to create connection",e)

    def Sumofindividual(self,job_3):
        try:
            sum_of_individual_final=""
            for iterrowsofemp in job_3:
                temp=iterrowsofemp[1] + " : " + str(iterrowsofemp[0])
                sum_of_individual_final=sum_of_individual_final+temp+"\n"
            return sum_of_individual_final
            logging.info("we have done with sum of individual salary")
        except Exception as e:
            logging.error("exception occured in the 3_rd query")

    def totalsumemp(self,total_sum_emp):
        try:
            total_sal=total_sum_emp.fetchone()
            total_final='TOTAL Salary of EMP'+" : "+str(total_sal[0])
            return total_final
            logging.info("we have done with total sum of employee")
        except Exception as e:
            logging.error("exception occured in the 2_nd query")


    def count_emp(self,total_count_emp):
        try:
            total_count=total_count_emp.fetchone()
            total='TOTAL_COUNT_EMP'+" : "+str(total_count[0])
            return total
            logging.info("we have done with total count of employee")
        except Exception as e:
            logging.error("exception occured in the 1_st query")

project_obj=Project()
project_obj.load_config()
project_obj.sqlconnect()