Feature: Reboot DHT network
    Scenario: 3 nodes reboot with timeout
        Given I have nodes with HDDs: 0.15, 0.15, 0.15
        When start network 
        And put data until HDDs have free capacity less than 50 
        Then I collect and remember DHT statistic
        When reboot all nodes one by one with timeout in 60 secs
        Then I collect DHT statistic and check with previous

    Scenario: 5 nodes reboot without timeout
        Given I have nodes with HDDs: 0.05, 0.05, 0.05, 0.05, 0.05
        When start network 
        And put data until HDDs have free capacity less than 70 
        Then I collect and remember DHT statistic
        When reboot all nodes one by one
        Then I collect DHT statistic and check with previous
