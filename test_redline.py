#!/usr/bin/env python


import redline
import unittest
import datetime


class ParameterizedTestCase(unittest.TestCase):
    def __init__(self, name, **parameters):
        unittest.TestCase.__init__(self, name)
        self.testParams = parameters


    def set(self, paramName, paramValue):
        self.testParams[paramName] = paramValue


    def param(self, paramName):        
        value = self.testParams.get(paramName)
        if not value:
            raise Exception('No parameter "%s" was registered with this test case.' % paramName)

        return value



class RedlineTestCase(ParameterizedTestCase):
    def __init__(self, name, **parameters):
        ParameterizedTestCase.__init__(self, name, **parameters)
        self.initialMessageCount = None
        self.updatedMessageCount = None
        self.redlineInstance = None
        self.configFilename = self.param('config_filename')
        self.hostname = self.param('hostname')
        self.config = None
        self.redisServer = None
        self.queueServer = None
        self.distributionPool = None


    def givenAValidRedlineInstance(self):
        self.config = redline.AppConfig(self.configFilename)
        self.redisServer = redline.RedisServer(self.hostname)
        self.queueServer = redline.QueueServer(self.config, self.redisServer)
        self.queueServer.purge()


    def givenADistributionPoolWithNSegments(self):
        poolName = self.param('pool_name')
        segments = self.param('pool_segments')
        poolConfig = redline.DistributionPoolConfig(poolName, segments)
        poolConfig.save(self.redisServer, self.config)



    def givenNQueuedMessages(self):
        self.initialMessageCount = self.queueServer.getMessageCount()
        self.updatedMessageCount = 0


    def whenAMessageIsQueued(self):
        currentDate = datetime.datetime.now()
        self.queueServer.queueMessage({ 'name': 'test_message_%s' % str(currentDate) })
        self.updatedMessageCount = self.queueServer.getMessageCount()


    def whenAMessageIsQueuedWithSegmentName(self, segmentName):
        currentDate = datetime.datetime.now()
        self.queueServer.queueMessage({ 'name': 'test_message_%s' % str(currentDate) }, segmentName)

    

    def whenNMessagesAreQueued(self, numMessages):
        for i in range(0, numMessages):
            self.whenAMessageIsQueued()


    def whenNMessagesAreQueuedWithPoolID(self, numMessages, poolID):
        for i in range(0, numMessages):
            segmentName = redline.DistributionPool(poolID, self.redisServer, self.config).nextSegment()
            self.whenAMessageIsQueuedWithSegmentName(segmentName)


    def thenTheQueueSizeIsIncremented(self):        
        self.assertTrue(self.updatedMessageCount == self.initialMessageCount + 1)


    def thenTheQueueSizeIsIncrementedBy(self, numMessages):
        self.assertTrue(self.updatedMessageCount == self.initialMessageCount + numMessages)


    def thenTheQueuedMessageIDHasTheSegmentName(self, segmentName):
        msg = self.queueServer.removeMostRecentlyQueuedMessage(segmentName)        
        self.assertTrue(msg.key.segment == segmentName)


    def thenTheQueuedMessagesAreDistributedOverAllSegments(self):
        poolSegments = redline.DistributionPool(self.param('pool_name'), self.redisServer, self.config)._loadSegments()
        keySegments = []
        for i in range(0, self.param('num_messages')):
            msg = self.queueServer.dequeueMessage()
            keySegments.append(msg.key.segment)
            self.assertTrue(msg.key.segment in poolSegments)

        for s in poolSegments:
            self.assertTrue(s in keySegments)
        



    def thenALoadedPoolContainsAllSegments(self):
        pool = redline.DistributionPool(self.param('pool_name'), self.redisServer, self.config) 
        print '>>> segment names: %s' % pool._loadSegments()

        self.assertTrue(pool.size == len(self.param('pool_segments')))
        for seg in pool._loadSegments():            
            self.assertTrue(seg in self.param('pool_segments'))        


    def test_add_single_message(self):
        self.givenAValidRedlineInstance()
        self.givenNQueuedMessages()
        self.whenAMessageIsQueued()
        self.thenTheQueueSizeIsIncremented()
        

    def test_add_n_messages(self):
        numMessages = self.param('num_messages')

        self.givenAValidRedlineInstance()
        self.givenNQueuedMessages()
        self.whenNMessagesAreQueued(numMessages)
        self.thenTheQueueSizeIsIncrementedBy(numMessages)


    def test_add_message_with_segment_designation(self):      
        segmentName = self.param('segment_name')
  
        self.givenAValidRedlineInstance()        
        self.whenAMessageIsQueuedWithSegmentName(segmentName)
        self.thenTheQueuedMessageIDHasTheSegmentName(segmentName)
        

    def test_distribution_pool_config(self):
        self.givenAValidRedlineInstance()
        self.givenADistributionPoolWithNSegments()
        self.thenALoadedPoolContainsAllSegments()


    def test_segment_rotation(self):
        self.givenAValidRedlineInstance()
        self.givenADistributionPoolWithNSegments()        
        self.whenNMessagesAreQueuedWithPoolID(self.param('num_messages'), self.param('pool_name'))
        self.thenTheQueuedMessagesAreDistributedOverAllSegments()
        

    def test_message_requeue(self):
        self.givenAValidRedlineInstance()
        self.whenAMessageIsQueued()
        self.whenTheMessageIsRequeued()
        self.thenTheRequeuedCountIsIncremented()
        

    def test_message_delay(self):
        self.givenAValidRedlineInstance()
        self.whenAMessageIsQueued()
        self.whenAMessageIsDequeuedWithDelay()
        self.thenTheDelayedSetContainsTheMessage()




def main():

    runner = unittest.TextTestRunner()
    test_suite = unittest.TestSuite()

    testParams = { 'config_filename' : 'redline_init_sample.yaml', 'hostname' : 'localhost' }
    testParams['pool_name'] = 'test_pool'
    testParams['pool_segments'] = ['seg1', 'seg2', 'seg3']
    testParams['segment_name'] = 'seg1'
    testParams['num_messages'] = 5

    test_suite.addTest(RedlineTestCase('test_add_single_message', **testParams))
    test_suite.addTest(RedlineTestCase('test_add_n_messages', **testParams))    
    test_suite.addTest(RedlineTestCase('test_add_message_with_segment_designation',  **testParams))
    test_suite.addTest(RedlineTestCase('test_distribution_pool_config', **testParams))
    test_suite.addTest(RedlineTestCase('test_segment_rotation', **testParams))


    runner.run(test_suite)

    


if __name__ == '__main__':
    main()
