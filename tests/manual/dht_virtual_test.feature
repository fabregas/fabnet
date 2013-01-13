Feature: DHT Init
    Scenario: Normal work
        When start virtual network with 40 nodes 
        Then I collect DHT statistic
        Then I put 1000 blocks (one block size - 1024 bytes)
        Then I collect DHT statistic
        Then I get and check all data blocks

        When start virtual network with 24 nodes
        And wait 40 seconds
        Then I collect DHT statistic
        Then I get and check all data blocks
        Then I collect topology from every node

        When stop 2 nodes
        And wait 140 seconds
        Then I collect DHT statistic
        Then I get and check all data blocks

        When stop 2 nodes
        And wait 5 seconds

        When stop 2 nodes
        And wait 5 seconds

        When stop 2 nodes
        And wait 5 seconds

        When stop 2 nodes
        And wait 5 seconds

        When stop 2 nodes
        And wait 5 seconds

        When stop 2 nodes
        And wait 5 seconds

        When stop 2 nodes
        And wait 240 seconds
        Then I collect DHT statistic
        Then I collect topology from every node

        When stop 15 nodes
        And wait 240 seconds
        Then I put 1000 blocks (one block size - 1024 bytes)


    Scenario: Failed half of network nodes
        When start virtual network with 32 nodes
        Then I collect DHT statistic
        Then I put 1000 blocks (one block size - 1024 bytes)
        Then I collect DHT statistic
        Then I get and check all data blocks

        When stop 16 nodes
        And wait 500 seconds
        Then I collect DHT statistic

