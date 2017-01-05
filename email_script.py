import mandrill
mandrill_client = mandrill.Mandrill('ym5n1GvWNOKLW60xfuHRFg')

def send_email(sender_address,sender_name,recipient_address,template_name,tag_list,subject_line):
    # var_vec needs to be of this form [{'name':'CURRENT_YEAR','content':'2015'},{'name':'Name','content':'Content'}]
    var_vec = []
    for tag_name,tag_content in tag_list.iteritems():
        var_vec.append({'name':tag_name,'content':tag_content})

    merge_vars =  [{'rcpt':recipient_address,'vars': var_vec}]

    message = { 'to': [{'email':recipient_address}],
                'global_merge_vars': [{'name':'Name','content':'Content'}],
                'merge_vars': merge_vars,
                'from_email': sender_address,
                'from_name': sender_name,
                'track_clicks':True,
                'track_opens':True,
                'tags':[template_name],
                'subject': subject_line }
    
    return mandrill_client.messages.send_template(template_name=template_name, template_content={}, message=message, async=False)
    import pg
    conn = pg.DB(host="10.223.192.6", user="etl", passwd="s0.Much.Data", dbname="analytics")
data = conn.query("select sum(case when gradescore='C' then 1 else 0 end) from ashim.eventcube_summary_raw_thez where enddate<=now()+interval '5 days' and enddate>=now()")
data2= conn.query("select sum(case when gradescore='C' then 0 else 1 end) from ashim.eventcube_summary_raw_thez where enddate<=now()+interval '5 days' and enddate>=now()")
data.getresult()
import pandas
from pandas import DataFrame
df = DataFrame(data.getresult())
df2 =DataFrame(data2.getresult())
PERCENT=((df2/(df+df2))*100).round(1)
perc = float(PERCENT.values[0])
perc
for i in range(0,12):
 send_email("datadawgs-reporting@doubledutch.me","The Dawgs", str(x.values[i,1]),"thez_version2",{'PERCENT':'%s%%' % perc,'USER_NAME':str(x.values[i,0])},"Thez- Events Activity Report")

 
