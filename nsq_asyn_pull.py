import sys
import json
import gnsq
import nsq


nsq_ip = ''
nsq_port = 4150

buf = []

class nsqAPI(object):

    def pull(self, topic, channel, runFunc, **args):
        try:
            def process_message(message):
                global buf
                message.enable_async()
                buf.append(message)
                if len(buf) >= 3:
                    for msg in buf:
                        msg.finish()
                    buf = []
                else:
                    # print 'deferring processing'
                    return
                result = json.loads(message.body)
                response = runFunc(result, **args)

            r = nsq.Reader(message_handler=process_message,
                        nsqd_tcp_addresses =["%s:%s" %(nsq_ip, nsq_port)],
                        topic=topic, channel=channel, max_in_flight=9)
            nsq.run()
        except Exception as e:
            print e
            return 

def runFunc(message, **args):
    # Pay attention: put your function here
    print message
    sys.exit(1)
