import sys
import csv
import re,itertools

relational_operators=["<", ">" ,"=", "<=", ">="]
schema={}


def read_metadata():
    metadata = open("./files/metadata.txt", "r")
    table_name=None
    count_col=-1
    for line in metadata.readlines():
        line=line.strip()   
        if line=="<begin_table>":
            table_name=None
        elif line=="<end_table>":
            count_col=-1
        elif count_col== -1:
            table_name=line.lower()
            schema[table_name]=[]
            count_col=0
        else:
            schema[table_name].append(line.lower())
            count_col+=1
    # for i,j in enumerate(schema):
    #   for k in schema[j]:
    #       print(i, j, k)

def query_breaker(query):
    query=query.strip()
    if len(query)<=2: #for ""
        print("SYNTAX ERROR: Empty query! Pass a query")
        exit(0)
    if query[-1]!=";":
        print("SYNTAX ERROR: Semi-colon expected at the end of the query")
        exit(0)
    
    query = query[:len(query)-1]#removes ;
    query=query.lower()
    query=query.split()
    # for x in query:
    #     print(x)
    if query[0]!='select':
        print("SYNTAX ERROR: Query should start with 'SELECT'")


    distinct_ind=select_ind=from_ind=where_ind=groupby_ind=orderby_ind=0
    select_count=0 #since select index is 0, keeping a count variable

    for index,word in enumerate(query):
        #print("\n", index, word)
        if word =="select":
            if select_count==1:
                print("SYNTAX ERROR: Only one 'SELECT' is expeceted in the query")
                exit(0)
            select_ind=index
            select_count=1

        if word=="from":
            if from_ind:
                print("SYNTAX ERROR: Only one 'FROM' is expected in the query")
                exit(0)
            from_ind=index

        if word=="where":
            if where_ind:
                print("SYNTAX ERROR: Only one 'WHERE' is expected in the query")
                exit(0)
            where_ind=index

        if word =="group":
            if groupby_ind:
                print("SYNTAX ERROR: Only one 'GROUP BY' is expected in the query")
                exit(0)
            if query[index+1]!="by":
                print("SYNTAX ERROR: 'BY' is expected after 'GROUP' in the query")
                exit(0)
            groupby_ind=index

        if word=='order':
            if orderby_ind:
                print("SYNTAX ERROR: Only one 'ORDER BY' is expected in the query")
                exit(0)
            if query[index+1]!="by":
                print("SYNTAX ERROR: 'BY' is expected after 'ORDER' in the query")
                exit(0)
            orderby_ind=index

        if word=='distinct':
            if distinct_ind:
                print("SYNTAX ERROR: Only one 'distinct' is expected in the query")
                exit(0)
            distinct_ind=index


    if select_count==0:
        print("SYNTAX ERROR: No 'SELECT' is passed in query")
        exit(0)
    if from_ind==0:
        print("SYNTAX ERROR: No 'FROM' is passed in the query")
        exit


    tables_arr=[]
    col_arr=query[select_ind+1:from_ind]
    conditions_arr=[]
    groupby_arr=[]
    orderby_arr=[]

    if where_ind==0:
        if groupby_ind==0:
            if orderby_ind==0:
                tables_arr += query[from_ind+1:]
                # for x in tables_arr:
                #     print(x)
            else:
                if from_ind>orderby_ind:
                    print("SYNTAX ERROR: 'ORDER BY' is expected next to 'FROM'")
                    exit(0)
                tables_arr+= query[from_ind+1:orderby_ind]
                orderby_arr+= query[orderby_ind+2:]

        else:
            if from_ind>groupby_ind:
                print("SYNTAX ERROR: 'GROUP BY' is expected next to 'FROM'")
                exit(0)

            if orderby_ind==0:
                tables_arr+= query[from_ind+1:groupby_ind]
                groupby_arr+=query[groupby_ind+2:]
                
            else:
                if groupby_ind>orderby_ind:
                    print("SYNTAX ERROR: 'ORDER BY' is expected next to 'GROUP BY'")
                    exit(0)

                tables_arr+= query[from_ind+1:groupby_ind]
                groupby_arr+=query[groupby_ind+2:orderby_ind]
                orderby_arr+=query[orderby_ind+2:]

    else:
        if from_ind>where_ind:
            print("SYNTAX ERROR: 'WHERE' is expected next to 'FROM'")
            exit(0)

        if groupby_ind==0:
            if orderby_ind==0:
                tables_arr+=query[from_ind+1: where_ind]
                conditions_arr+= query[where_ind+1:]
            else:
                if where_ind>orderby_ind:
                    print("SYNTAX ERROR: 'ORDER BY' is expected next to 'WHERE'")
                    exit(0)

                tables_arr+= query[from_ind+1: where_ind]
                conditions_arr+= query[where_ind+1: orderby_ind]
                orderby_ind+= query[orderby_ind+2:]
        else:
            if where_ind>groupby_ind:
                print("SYNTAX ERROR: 'GROUP BY' is expected next to 'WHERE'")
                exit(0)

            if orderby_ind==0:
                tables_arr+= query[from_ind+1: where_ind]
                conditions_arr+= query[where_ind+1: groupby_ind]
                groupby_arr+= query[groupby_ind+2:]
            else:
                if groupby_ind>orderby_ind:
                    print("SYNTAX ERROR: 'ORDER BY' is expected next to 'GROUP BY'")
                    exit(0)
                tables_arr+= query[from_ind+1: where_ind]
                conditions_arr+= query[where_ind+1: groupby_ind]
                groupby_arr+= query[groupby_ind+2: orderby_ind]
                orderby_arr+= query[orderby_ind+2:]

    if tables_arr==[] and from_ind>0:
        print("SYNTAX ERROR: Table(s) not given after 'FROM'")
        exit(0)
    if conditions_arr==[] and where_ind>0:
        print("SYNTAX ERROR: Condition(s) not given after 'WHERE'")
        exit(0)
    if groupby_arr==[] and groupby_ind>0:
        print("SYNTAX ERROR: Column(s) not given after 'GROUP BY'")
        exit(0)
    if orderby_arr==[] and orderby_ind>0:
        print("SYNTAX ERROR: Column(s) not given after 'ORDER BY'")
        exit(0)

    if select_count==1 and col_arr==[]:
        print("SYNTAX ERROR: Column(s) not given after 'SELECT'")
        exit(0)

    #print("\n",tables_arr, col_arr, conditions_arr, groupby_arr, orderby_arr, distinct_ind)
    return tables_arr, col_arr, conditions_arr, groupby_arr, orderby_arr, distinct_ind

def parse_table(tables_arr):
    tables_given=[]
    for i in tables_arr:
        if i in schema.keys():
            tables_given.append(i)
        else:
            print("'{}' table is not found in metadata".format(i))
            exit(0)
    #print(tables_given)
    return tables_given

def parse_columns(tables_arr, col_arr):
    columns_given=[]
    for i in col_arr:
        check_reg=re.match("(.*)\((.*)\)",i)
        if check_reg:
            aggregate, i=check_reg.groups()
        else:
            aggregate= None
        if i== '*':
            if check_reg:
                print("SYNTAX ERROR: Aggregate can't be used on '*'")
                exit(0)
            else:
                for j in tables_arr:
                    for k in schema[j]:
                        columns_given.append(([j],[k],aggregate))
                        #appending table, attribute and aggregate
                        #print(i,j, aggregate)

        else:
            matched_table=tables_arr[0]
            count_col=0
            for j in tables_arr:
                if i in schema[j]:
                    if count_col>0:
                        print("SYNTAX ERROR: Column names are unique in tables.")
                        exit(0)
                    count_col+=1
                    matched_table=j
                    
            if count_col:
                columns_given.append(([matched_table],[i],aggregate))
                #appending table, column, aggregate
                #print([matched_table],i,aggregate)
            else:
                print("SYNTAX ERROR: Given column name is not found")
                exit(0)
    #print(columns_given)
    return columns_given

def parse_condition(tables_arr, conditions_arr):
    conditions_given=[]
    conditional_operators= None
    if len(conditions_arr)==0:
        return conditions_given, conditional_operators

    if "and" in conditions_arr:
        conditions_arr=conditions_arr.split(" and ")
        conditional_operators="and"
    if "or" in conditions_arr:
        #print(conditions_arr, conditional_operators)
        conditions_arr=conditions_arr.split(" or ")
        conditional_operators="or"
        #print(conditions_arr, conditional_operators)
    else:
        conditions_arr=[conditions_arr]

    for i in conditions_arr:
        count_comparator=0
        comparator=''
        for j in relational_operators:
            if j in i:
                comparator=j
                count_comparator=1
                # print("Entered count")
                break

        if count_comparator==0:
            print("SYNTAX ERROR: Comparator is not given in the query")
            exit(0)

        left_variable, right_variable=i.split(comparator)
        variables=[left_variable, right_variable]
        token_condition=[comparator]

        for j in variables:
            if (j.isdigit())==False:
                matched_table=''
                count_match=0
                for k in tables_arr:
                    if j in schema[k]:
                        if count_match==True:
                            print("SYNTA ERROR: Column names(Identifier) must be unique in condition")
                            exit(0)

                        matched_table=k
                        count_match=1
                if count_match==0:
                    print("SYNTAX ERROR: Column name(Identifier) is not recognized")
                    exit(0)
                token_condition.append([matched_table, variables])
            else:
                token_condition.append(["Integer",j])

        conditions_given.append(token_condition)
    print("conditions giv: ",conditions_given, conditional_operators)
    return conditions_given, conditional_operators


def parse_groupby(tables_arr, col_arr, groupby_arr):
    groupby_table=None
    col_groupby=None
    if len(groupby_arr)==0:
        return groupby_table, col_groupby

    col_groupby=groupby_arr[0]
    count_col=0

    for i in tables_arr:
        if col_groupby in schema[i]:
            if count_col==1:
                print("SYNTAX ERROR: Same columns are given for groupby in the query")
                exit(0)

            groupby_table=i
            count_col=1
    if count_col==0:
        print("SYNTAX ERROR: Column not given for the groupby in the query")
        exit(0)

    return groupby_table, col_groupby

def run_query(tables_given, columns_given, col_required, conditions_given, conditional_operators, groupby_table, col_aggregated, col_projected, col_orderby, distinct_ind):

    index_joining={}
    count_distinct_col=0
    final_tables =[[] for table in tables_given]

    for ind, table in enumerate(tables_given):
        file="files/" +table+".csv"
        # print(file)
        with open(file) as fp:
            col_list=list(csv.reader(fp, delimiter=','))
            table_typecast=[[] for i in col_list]
            for ind2, row in enumerate(col_list):
                for ind3, value in enumerate(row):
                    table_typecast[ind2].append(int(value))
        col_list=table_typecast

        for ind2, row in enumerate(col_list):
            data=[]
            for i in col_required[table]:
                current_index=schema[table].index(i)#column
                data.append(row[current_index])
            final_tables[ind].append(data)

        temp_col={}
        for ind2, col in enumerate(col_required[table]):
            temp_col[col] = count_distinct_col
            count_distinct_col+=1
        index_joining[table]=temp_col

    #joining of tables in query

    table_joined=[]
    for row in itertools.product(*final_tables):
        row_join=[]
        for each_box in row:
            for value in each_box:
                row_join.append(value)
        table_joined.append(row_join)

    table_output=[]

    #where
    temp_table_where=table_joined
    if len(conditions_given)!=0:
        temp_table_where=[]
        truth_table=[]
        for row in table_joined:
            truth_row=[]
            for i in conditions_given:
                truth_row.append(True)
            truth_table.append(truth_row)


        counter=0
        for ind, condition in enumerate(conditions_given):
            col_compare=[]
            for table, column in condition[1:]:
                col_index=0
                if table!="Integer":
                    col_index=index_joining[table][column]
                col_values=[]
                for row in table_joined:
                    if table=="Integer":
                        col_values.append([int(column)])
                    else:
                        col_values.append([row[col_index]])
                col_compare.append(col_values)

            for i, row in enumerate(table_joined):
                if condition[0]=='>':
                    if col_compare[0][i]<= col_compare[1][i]:
                        truth_table[i][counter]=False
                if condition[0]=='<':
                    if col_compare[0][i]>= col_compare[1][i]:
                        truth_table[i][counter]=False
                if condition[0]=='=':
                    if col_compare[0][i] != col_compare[1][i]:
                        truth_table[i][counter]=False
                if condition[0]=='>=':
                    if col_compare[0][i] < col_compare[1][i]:
                        truth_table[i][counter]=False
                if condition[0]=='<=':
                    if col_compare[0][i] > col_compare[1][i]:
                        truth_table[i][counter]=False
            counter+=1

        for i, row in enumerate(table_joined):
            if conditional_operators=="or":
                if truth_table[i][0] or truth_table[i][1]:
                    temp_table_where.append(row)
            elif conditional_operators=='and':
                if truth_table[i][0] and truth_table[i][1]:
                    temp_table_where.append(row)
            else:
                if truth_table[i][0]:
                    temp_table_where.append(row)

    #groupby

    check_groupby=0
    temp_table_groupby=temp_table_where
    if groupby_table:
        check_groupby=1
        temp_table_groupby=[]
        col_projected_index=-1
        found={}
        for table, col in col_required.items():
            for i in col:
                if i == col_projected:
                    col_projected_index=index_joining[table][i]
                    break
        if col_projected_index==-1:
            print("SYNTAX ERROR: Given column is not found for groupby in joined table")
            exit(0)
        for ind, row in enumerate(temp_table_where):
            val=row[col_projected_index]
            if val in found.keys():
                found[val].append(ind)
            else:
                found[val]=[]
                found[val].append(ind)
        for key,row_id in found.items():
            temp_row=[]
            for table, col, aggregate in columns_given:
                if col[0]==col_projected:
                    temp_row.append(key)
                    continue
                if aggregate==None:
                    print("SYNTAX ERROR: The queried columns should have aggregation")
                    exit(0)

                col_index=index_joining[table[0]][col[0]]
                aggre=[]
                aggre_value=None
                for row_ind in row_id:
                    aggre.append(temp_table_where[row_ind][col_index])
                if aggregate=='sum':
                    aggre_value= sum(aggre)
                if aggregate=='average':
                    aggre_value=sum(aggre)/len(aggre)
                if aggregate=='max':
                    aggre_value=max(aggre)
                if aggregate=='min':
                    aggre_value=min(aggre)
                if aggregate=='count':
                    aggre_value=len(aggre)
                if aggregate=='None':
                    print("SYNTAX ERROR: Given aggregate function not found")
                    exit(0)
                temp_row.append(aggre_value)
            temp_table_groupby.append(temp_row)


    #allaggregate
    temp_table_aggregate=temp_table_groupby
    count_aggregate=0
    for table, col, aggregate in columns_given:
        if aggregate!=None:
            count_aggregate+=1
    if check_groupby==0 and count_aggregate>0:
        if count_aggregate<len(columns_given):
            if groupby_table==None:
                print("SYNTAX ERROR: Aggregate used on only a subset of columns, table is not derivable")
                exit(0)
        else:
            temp_table_aggregate=[]
            temp_row=[]
            for table, col, aggregate in columns_given:
                col_index=index_joining[table[0]][col[0]]
                aggre=[]
                aggre_value=None
                for row in temp_table_where:
                    aggre.append(row[col_index])
                if aggregate=='sum':
                    aggre_value=sum(aggre)
                if aggregate=='average':
                    aggre_value=sum(aggre)/len(aggre)
                if aggregate=='max':
                    aggre_value=max(aggre)
                if aggregate=='min':
                    aggre_value=min(aggre)
                if aggregate=='count':
                    aggre_value=len(aggre)
                if aggregate=='None':
                    print("SYNTAX ERROR: Given aggregate function is not found")
                    exit(0)
                temp_row.append(aggre_value)
            temp_table_aggregate.append(temp_row)



    #select
    
    temp_table_select=temp_table_aggregate
    if check_groupby==0 and count_aggregate==0:
        temp_table_select=[]
        for row in temp_table_where:
            temp_row=[]
            for table, column, aggregate in columns_given:
                col_index=index_joining[table[0]][column[0]]
                temp_row.append(row[col_index])
            temp_table_select.append(temp_row)
    #print(temp_table_select)



    #distinct
    temp_table_distinct= temp_table_select
    if distinct_ind>0:
        temp_table_distinct=[]
        for row in temp_table_select:
            if row not in temp_table_distinct:
                temp_table_distinct.append(row)

    #orderby

    temp_table_orderby=temp_table_distinct
    if col_orderby:
        orderby_col_ind=-1
        for ind, (table, col, aggregate) in enumerate(columns_given):
            if col[0]==col_orderby[0]:
                orderby_col_ind=ind
                break
        if orderby_col_ind==-1:
            print("SYNTAX ERROR: The column to orderby is not found in joined table")
            exit(0)

        if len(col_orderby)==1 or col_orderby[1]=='asc':
            temp_table_orderby=sorted(temp_table_orderby, key=lambda table:table[orderby_col_ind])
        elif col_orderby[1]=='desc':
            temp_table_orderby=sorted(temp_table_orderby, key=lambda table:table[orderby_col_ind], reverse=True)
        else:
            print("SYNTAX ERROR: Given sorting option doesn't match 'ASC' or 'DESC'")
            exit(0)
    table_output=temp_table_orderby

    # print(table_output)

    #print
    count_col=len(columns_given)
    counter=0
    for table, column, aggregate in columns_given:
        counter+=1
        if aggregate==None:
            if counter<count_col:
                temp=column[0]
                print(table[0]+'.'+temp.upper(), end=',')
            else:
                temp=column[0]
                print(table[0]+'.'+temp.upper())
        else:
            if counter<count_col:
                temp=column[0]
                print(aggregate+'('+table[0]+'.'+temp.upper()+')', end=',')
            else:
                temp=column[0]
                print(aggregate+'('+table[0]+'.'+temp.upper()+')')

    for i in table_output:
        count=0
        for j in i:
            count+=1
            if count<count_col:
                print(j,end=',')
            else:
                print(j)


def main():
    if len(sys.argv)==2:
        read_metadata()
        parsables=query_breaker(sys.argv[1])
        #print(parsables)

        tables_arr="".join(parsables[0]).split(',')
        col_arr=''.join(parsables[1]).split(',')
        conditions_arr=''.join(parsables[2])
        groupby_arr=parsables[3]
        orderby_arr=parsables[4]
        distinct_ind=parsables[5]

        tables_given=parse_table(tables_arr)
        columns_given=parse_columns(tables_arr,col_arr)
        col_required={table:[] for table in tables_given}
        for i in tables_given:
            for j in schema[i]:
                col_required[i].append(j)

        conditions_given, conditional_operators=parse_condition(tables_arr, conditions_arr)
        groupby_table, col_projected=parse_groupby(tables_arr, col_arr, groupby_arr)

        col_orderby=[]
        if len(orderby_arr)==0:
            col_orderby=None
        else:
            col_orderby=orderby_arr
            if len(col_orderby)>2:
                print("SYNTAX ERROR: Arguments usage error of Orderby, '...order by <column> ASC/DESC'")
                exit(0)

        col_aggregated=[]
        for i in col_arr:
            check_reg=re.match("(.*)\((.*)\)",i)
        if check_reg:
            aggregate, i=check_reg.groups()
            col_aggregated.append(i)


        # print(tables_given)
        # print(columns_given, col_required)
        # print(conditions_given, conditional_operators)
        # print(groupby_table, col_aggregated, col_projected)
        # print(col_orderby)

        run_query(tables_given,columns_given,col_required,conditions_given,conditional_operators,groupby_table, col_aggregated, col_projected, col_orderby, distinct_ind)

    else:
        print("INVALID INPUT FORMAT: Use './2019101104.sh 'select .. from ..;'")
        exit(0)

main()


# query_breaker("select e from table1, table2 where a>d or a<b")
# parse_columns(['table1'],['sum(a)'])
# # parse_table(['hellow'])
# parse_condition(['table1', 'table2'])