#!/bin/sh
cd `dirname $0`
ROOT_PATH=`pwd`
java -Dtalend.component.manager.m2.repository=$ROOT_PATH/../lib -Xms256M -Xmx1024M -cp classpath.jar: local_project.load_fact_booking_0_1.Load_Fact_Booking --context=Default "$@" 