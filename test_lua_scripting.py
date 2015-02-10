#/usr/bin/env python

import redline
import redis



def loadScriptFile(filename):
    script = None
    with open('lua/queue_msg.lua', 'r') as scriptFile:
        script = scriptFile.read() 
    
    return script



def testQueueMsg(server, config):
    script = loadScriptFile('lua/queue_msg.lua')
    queueMsg = server.instance.register_script(script)

    print queueMsg(keys=[config.valuesTableName(), 
            config.pendingListName(), 
            config.messageStatsTableName()], 
            args=['1021', {'foo':'purpleline_foo'}])    



def main():
    rServer = redline.RedisServer('localhost')
    config = redline.AppConfig('redline_init_sample.yaml')
    testQueueMsg(rServer, config)



           



if __name__== '__main__':
   main()
