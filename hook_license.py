import pbs
import sys
import re
import sqlite3
import datetime
import os
import pwd
import grp

sqlite_db="/var/spool/pbs/license.db"

licenses_list = ["ansys", "ansys-cfd", "ansys-hpc", "ansys-mcad", "ansys-me", "clcgenomics", "fluent", "gridmathematica", "maple10", "maple10p", "maple11", "marc", "marcn", "mathematica", "matlab", "matlab_Bioinformatics_Toolbox", "matlab_Communication_Toolbox", "matlab_Compiler", "matlab_Control_Toolbox", "matlab_Curve_Fitting_Toolbox", "matlab_Data_Acq_Toolbox", "matlab_Database_Toolbox", "matlab_Datafeed_Toolbox", "matlab_Distrib_Computing_Toolbox", "matlab_Econometrics_Toolbox", "matlab_Embedded_IDE_Link", "matlab_Excel_Link", "matlab_Financial_Toolbox", "matlab_Fin_Instruments_Toolbox", "matlab_Fixed_Point_Toolbox", "matlab_Fuzzy_Toolbox", "matlab_GADS_Toolbox", "matlab_Identification_Toolbox", "matlab_Image_Acquisition_Toolbox", "matlab_Image_Toolbox", "matlab_Instr_Control_Toolbox", "matlab_MATLAB_Builder_for_Java", "matlab_MATLAB_Coder", "matlab_MATLAB_Distrib_Comp_Engine", "matlab_Neural_Network_Toolbox", "matlab_Optimization_Toolbox", "matlab_PDE_Toolbox", "matlab_Power_System_Blocks", "matlab_RTW_Embedded_Coder", "matlab_Signal_Blocks", "matlab_Signal_Toolbox", "matlab_SimBiology", "matlab_SimHydraulics", "matlab_SimMechanics", "matlab_Simscape", "matlab_SIMULINK", "matlab_Simulink_Control_Design", "matlab_Simulink_HDL_Coder", "matlab_Statistics_Toolbox", "matlab_Symbolic_Toolbox", "matlab_Target_Support_Package", "matlab_Vehicle_Network_Toolbox", "matlab_Video_and_Image_Blockset", "matlab_Virtual_Reality_Toolbox", "matlab_Wavelet_Toolbox"]

try:
	
    e = pbs.event()
    if e.type == pbs.RUNJOB:
        j = e.job
        
        pbs.logmsg(pbs.EVENT_DEBUG, "%s has Resource_List: %s" % (j.id, str(j.Resource_List)))
        
        conn = sqlite3.connect(sqlite_db)
        c = conn.cursor()
                        
        for license in licenses_list:
            try:
                num = j.Resource_List[license]
            except:
                pbs.logmsg(pbs.EVENT_DEBUG, "license not defined: %s" % license)
                num = None

            if num != None:
                # pokud tabulka neexistuje, tak ji vytvorim
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='licenses'");
                if c.fetchone() == None:
                    c.execute("CREATE TABLE licenses (date text, job_id text, job_owner text, license_name text, value integer)")

                #vlozim licenci do tabulky a koncim
                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                c.execute("INSERT INTO licenses VALUES ('%s', '%s', '%s', '%s', %d)" % (now, j.id, j.Job_Owner, license, int(num)))

        conn.commit()
        conn.close()

except SystemExit:
    pass
except:
    e.reject("license hook failed")
