Feature: Pull subranges tests
    Scenario: 4 nodes test...
        Given I have nodes with HDDs: 1.5, 1, 1, 2
        When start network 
        And put data until HDDs are full
        Then maximum HDD free space less than 10

    Scenario: 8 nodes test...
        Given I have nodes with HDDs: 1.5, 1, 1, 2, 2, 1, 1.5, 2
        When start network 
        And put data until HDDs are full
        Then maximum HDD free space less than 10

