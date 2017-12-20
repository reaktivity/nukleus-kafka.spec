# Descriptions of tests

### fetch.v5

nature: client=sse (client) side only, server=kafka (server) side only, both=client and server, server\*=server only but used with client script from the test above

name                                    | nature   | description
--------------------------------------- | -------- | -----------
begin.ext.missing, client               | client   | no sse ext data on begin results in reset
distinct.offset.messagesets.fanout      | both     | client1 subscribes at offset=1 receives msg1 notifies client2 to connect receives msg2; client2 subscribes at offset=0 receives msgs 1 2 3
fanout.with.historical.message          | both     | client1 subscribes at offset=1 receives msg1 notifies client2 to connect awaits signal for server to send live msg2, receives msg2; client2 subscribes at offset=0 receives historical msg 1 notifies server to send live msg2 receives msg2
fanout.with.historical.messages         | both     | client script is same as above, server historical fetch yields msg1 and msg2
fanout.with.slow.consumer               | both     | clients 1 and 2 connect and subscribe in that order; client2 has small window can only receive one msg at a time, receives msg1 and msg2 then notifies server to deliver second live response ; server fetches 2 live messages can only deliver msg1 to client2 so does historical fetch to get msg2 then live fetches msg3
invalid.topic.name                      | client   | invalid character in topic name results in reset
nonzero.offset                          | both     | 
nonzero.offset.message                  | both     | 
nonzero.offset.messages                 | both     | 
unknown.topic.name                      | client   | unknown topic name results in reset
zero.offset                             | both     | 
zero.offset.message                     | both     | 
zero.offset.messages                    | both     | 
zero.offset.messages.fanout             | both     | 
zero.offset.messages.multiple.partitions| both     | 
zero.offset.messages.multiple.nodes     | server\* |
zero.offset.messagesets                 | both     | 
zero.offset.messagesets.fanout          | both     | 
