%~d0
cd %~dp0
java -Dtalend.component.manager.m2.repository="%cd%/../lib" -Xms256M -Xmx1024M -cp classpath.jar; local_project.load_fact_booking_0_1.Load_Fact_Booking --context=Default %* 