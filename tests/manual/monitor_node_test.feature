Feature: Monitor node
    Scenario: Normal work
        Given I have nodes with HDDs: 0.1, 0.1, 0.1, 0.1
        And clear monitoring stat
        When start network 
        And start monitor
        And wait 85 seconds
        Then see collected stats for all nodes

