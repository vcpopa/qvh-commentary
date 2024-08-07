from utils import read_sql,connection,execute_stored_proc


if __name__ =="__main__":

    Table_Pro_Current  = """

    SELECT * from  [dosapp].[Apps_Input_TableProComponent_Current]
    """

    Table_Pro_History   = """

    SELECT * from  [dosapp].[Apps_Input_TableProComponent]
    """

    TextBox_Current  = """

    SELECT * from  [dosapp].[Apps_Input_TextBoxComponent_Current]
    """

    TextBox_History   = """

    SELECT * from  [dosapp].[Apps_Input_TextBoxComponent]
    """

    DropDownLists = """
    SELECT * from [dosapp].[Apps_Input_DropdownList]
    """
    
    df_Table_Pro_Current = read_sql(Table_Pro_Current,db_name="public-dos-connectionstring")
    df_Table_Pro_History = read_sql(Table_Pro_History, db_name="public-dos-connectionstring")
    df_TextBox_Current = read_sql(TextBox_Current, db_name="public-dos-connectionstring")
    df_TextBox_History = read_sql(TextBox_History, db_name="public-dos-connectionstring")
    df_DropDownLists = read_sql(DropDownLists, db_name="public-dos-connectionstring")

    with connection(db_name='public-dataflow-connectionstring') as conn:
        df_Table_Pro_Current.to_sql('Apps_Input_TableProComponent_Current', conn, schema='staging', if_exists='replace', index=False)
        df_Table_Pro_History.to_sql('Apps_Input_TableProComponent', conn, schema='staging', if_exists='replace', index=False)
        df_TextBox_Current.to_sql('Apps_Input_TextBoxComponent_Current', conn, schema='staging', if_exists='replace', index=False)
        df_TextBox_History.to_sql('Apps_Input_TextBoxComponent', conn, schema='staging', if_exists='replace', index=False)
        df_DropDownLists.to_sql('Apps_Input_DropDownLists', conn, schema='staging', if_exists='replace', index=False)



    query = """

    with cohort as (
    SELECT
        case when AppTitle like 'KSO%' then LEFT(AppTitle, 4) else 'DEV' end  AS Area_ID,
        AppTitle AS CommentarySection,
        AppTable AS CommentaryBox,
        Measure_Description, 
        ReferenceValue, 
        CommentaryLevel, 
        Period, 
        TableRow AS OrderNo,
        case when ColumnTitle = 'Risk / Issues' then 'Risks / Issues' else ColumnTitle end as ColumnTitle ,
        ColumnValue,
        LastSaved,
        case when AppTitle like '%Non-Annual Goal%' then 'NonAnnual Goal'
            when AppTitle like '% Annual Goal%' then 'Annual Goal'
            else null end as AnnualGoal,
        Dashboard

    FROM
         [staging].[Apps_Input_TableProComponent]
    WHERE rowid = 1
    )
    select *,CommentaryLevel + '|' +  Dashboard  as Key1  from cohort
    where eomonth(period)  >= eomonth(dateadd(month, -2, getdate()))


    ;
    """
    df_Table = read_sql(query, db_name='public-dataflow-connectionstring')
    unique_boxes = df_Table['CommentaryBox'].drop_duplicates()
    for box in unique_boxes:
        # Filter DataFrame for the current CommentaryBox
        filtered_df = df_Table[df_Table['CommentaryBox'] == box]
        # Pivot the filtered DataFrame
        pivot_df = filtered_df.pivot_table(
            index=['Area_ID', 'CommentarySection', 'CommentaryBox', 'OrderNo', 'Measure_Description', 'Period', 'LastSaved', 'CommentaryLevel', 'Dashboard','Key1'],
            columns='ColumnTitle',
            values='ColumnValue',
            aggfunc='first'
        ).reset_index()

        # Define the table name based on CommentaryBox
        table_name = f"Commentary_Tables_{box.replace(' ', '_')}"

        # If CommentaryBox is 'Actions', create tables based on AnnualGoal
        if box == 'Actions':
            unique_annual_goals = filtered_df['AnnualGoal'].dropna().unique()
            for goal in unique_annual_goals:
                goal_filtered_df = filtered_df[filtered_df['AnnualGoal'] == goal]
                goal_pivot_df = goal_filtered_df.pivot_table(
                    index=['Area_ID', 'CommentarySection', 'CommentaryBox', 'OrderNo', 'Measure_Description', 'Period', 'LastSaved','CommentaryLevel', 'Dashboard','Key1'],
                    columns='ColumnTitle',
                    values='ColumnValue',
                    aggfunc='first'
                ).reset_index()
                goal_table_name = f"Commentary_{goal.replace(' ', '_')}_{box.replace(' ', '_')}"
                with connection(db_name="public-dataflow-connectionstring") as conn:
                    goal_pivot_df.to_sql(goal_table_name, conn, schema='staging', if_exists='replace', index=False)
        else:
            with connection(db_name="public-dataflow-connectionstring") as conn:
                pivot_df.to_sql(table_name, conn, schema='staging', if_exists='replace', index=False)

    query = """

    with cohort as (
        SELECT
        case when AppTitle like 'KSO%' then LEFT(AppTitle, 4) else 'DEV'  end  AS Area_ID,
        AppTitle AS CommentarySection,
        AppTextBox AS CommentaryBox,
        Measure_Description, 
        ReferenceValue, 
        CommentaryLevel, 
        Period, 
        App_Text as Text ,
        LastSaved,
        LTRIM(substring(Apptitle, 5, len(Apptitle ))) CommentaryType,
        --case when LTRIM(substring(Apptitle, 5, len(Apptitle ))) like 'Annual Goal%' then 'Annual Goal'
          --  when LTRIM(substring(Apptitle, 5, len(Apptitle ))) like 'Non-Annual Goal' then 'NonAnnual Goal'
--            else null end as AnnualGoal ,
        case when AppTitle like '%Non-Annual Goal%' then 'NonAnnual Goal'
            when AppTitle like '% Annual Goal%' then 'Annual Goal'
            else null end as AnnualGoal,
        DATEADD(month, DATEDIFF(month, 0, CAST(LastSaved AS DATE)) - 1, 0) as CommentaryMonth,
        Dashboard 
    FROM
        
        [staging].[Apps_Input_TextBoxComponent]
    where RowID = 1
        )
        SELECT *,CommentaryLevel + '|' +  Dashboard  as Key1  from cohort
        where eomonth(coalesce(period, CommentaryMonth))  >= eomonth(dateadd(month, -2, getdate()))
    """
    df_Table = read_sql(query, db_name='public-dataflow-connectionstring')
    with connection(db_name='public-dataflow-connectionstring') as conn:
        df_Table.to_sql('Commentary_TextBoxes', conn, schema='staging', if_exists='replace', index=False)



    execute_stored_proc("EXEC scd.UpdateCommentary")
