# XIAO W5500 WiFi Connection
The XIAO W5500 Ethernet Adapter is a ompact PoE development board featuring the XIAO ESP32S3 Plus, with an integrated, isolated PoE module and TPS563201-based power conversion that delivers a clean 5V supply to power the microcontroller. Ideal for IoT projects, where a versatile combination of reliable Ethernet connectivity and low-power wireless processing simplifies installation and enhances system performance.

Here the WiFi connection is used to get a wireless connection between the local broker and the router.

## Adjusting transmit power of XIAO W5500 Adapter

To get a good network signal it is possible to add a code segment for adjusting power inside all codes that use WiFi, 
and by adjusting the transmit power, the signal strength can be significantly improved.

Please have a look to the added lines of code in the setup() function.
