# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

def main(argv):
    import argparse, os.path, io, json

    parser = argparse.ArgumentParser(description=f"Service Bus receiver")
    parser.add_argument('--sbcreds', type=str, help=f"Service Bus credentials file",default=os.path.join(os.path.dirname(__file__),f"creds_sb_{os.environ['USERNAME']}.json"))
    parser.add_argument('-count', type=int, help=f"Count of messages to receive, default to no limit", default=None)
    parser.add_argument('-seq', type=int, help=f"Sequence number of first message to receive", default='0')
    parser.add_argument('-save', help=f"Save messages to files", action='store_true')
    parser.add_argument('-savedir', type=str, help=f"directoty to save messags to, defaults to msgs", default='msgs')
    parser.add_argument('-savefmt', type=str, help=f"Save messages format, individual JSONs, single JSON or CSV",  choices=['JSONs','JSON','CSV'], default='JSONs')
    args = parser.parse_args(argv[1:])

    try:
        import azure.servicebus as sb
    except:
        print(f"Cannot import azure.servicebus, run pip install azure-servicebus --pre")
        import azure.servicebus as sb

    from azure.servicebus import ServiceBusClient as sbc

    print(f"Using {sb.__name__} version {sb.__version__}")

    with io.open(args.sbcreds) as f:
        conn_data = json.load(f)

    conn_str=';'.join([f"{k}={v}" for k,v in conn_data.items()][:-1])
    print(f"Connecting with {conn_str}")

    with sbc.from_connection_string(conn_str=conn_str,logging_enable=False) as cl:
        qn=f"{conn_data['EntityPath']}/ConsumerGroups/$Default/Partitions/0"
        print(f"Got cl={cl}, using qn={qn}")
        qr=cl.get_queue_receiver(queue_name=qn)
        print(f"Got qr={qr}")

        msgs=[]

        msg=qr.next()
        count=args.count
        minSeq=None
        maxSeq=None
        while msg is not None and ((count is None) or count>0):
            bodies=b''.join(msg.body)
            if args.save and args.savefmt.upper()=='JSONS':
                with io.open(os.path.join(args.savedir,f"msg_{msg.sequence_number}.json"),'wb') as f:
                    f.write(bodies)
            try:
                for body in bodies.split(b'\r\n'):
                    msgBody=json.loads(body)
                    print(f"seq_num={msg.sequence_number} {msg.enqueued_time_utc} {msg.delivery_count} body keys#={len(msgBody.keys())} app_props={msg.application_properties}")
                    if args.save and args.savefmt.upper() in ('JSON','CSV'):
                        msgBody['sequence_number']=msg.sequence_number
                        #msgBody['enqueued_sequence_number']=msg.enqueued_sequence_number
                        msgBody['enqueued_time_utc']=msg.enqueued_time_utc
                        #msgBody['delivery_count']=msg.delivery_count
                        #msgBody['delivery_count']=msg.delivery_count
                        #msgBody['message_id']=msg.message_id
                        #msgBody['partition_key']=msg.partition_key
                        #msgBody['session_id']=msg.session_id
                        #msgBody['subject']=msg.subject
                        #msgBody['time_to_live']=msg.time_to_live
                        #msgBody['to']=msg.to
                        msgs.append(msgBody)
                        if minSeq is None: minSeq=msg.sequence_number
                        elif minSeq>msg.sequence_number: minSeq=msg.sequence_number
                        if maxSeq is None: maxSeq=msg.sequence_number
                        elif maxSeq<msg.sequence_number: maxSeq=msg.sequence_number
            except Exception as exc:
                import traceback
                traceback.print_exc()

            msg=qr.next()
            if count is not None:
                count=count-1
        if len(msgs)>0:
            if args.save and args.savefmt.upper()=='JSON':
                with io.open(os.path.join(args.savedir,f"msg_{minSeq}-{maxSeq}.json"),'wb') as f:
                    json.dump(msgs,f)
            if args.save and args.savefmt.upper()=='CSV':
                import pandas as pd
                df=pd.DataFrame.from_dict(msgs)
                print(f"Storing shape={df.shape} msgs seq {minSeq} to {maxSeq} into msg_{minSeq}-{maxSeq}.csv")
                df.to_csv(f"msg_{minSeq}-{maxSeq}.csv",index=False)
if __name__ == "__main__":
    import sys
    main(sys.argv)
