create or replace procedure sim(T_NAME varchar)
        returns varchar
        language javascript
        as 
        $$
        var sql_stm = "create view  F1_DWH.F1_STAGE.STG as select * from  F1_DWH.F1_STAGE."+T_NAME+";"
        try{
        var stm_exec =  snowflake.createStatement({sqlText:sql_stm});
        var result_set0 = stm_exec.execute();
        return "view created sucessfuly for"+T_NAME+":stg_"+T_NAME;
        }

    
    catch(err)
        {
        return err.MESSAGE
        }
        $$;



call sim('circuits');
