Feature: DHT Init
    Scenario: Normal work
        When start virtual network with 4 nodes 
        Then I collect DHT statistic
        Then I put 1000 blocks (one block size - 1024 bytes)
        Then I collect DHT statistic
        Then I get and check all data blocks

        When start virtual network with 12 nodes
        And wait 40 seconds
        Then I collect DHT statistic
        Then I get and check all data blocks
        Then I collect topology from every node

        When stop 5 nodes
        And wait 120 seconds
        Then I collect DHT statistic
        Then I get and check all data blocks
        Then I collect topology from every node

