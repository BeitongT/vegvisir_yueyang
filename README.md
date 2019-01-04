#vegvisir

Run java -jar keygen.jar [password] to generate RSA key pairs for the certificate authority.

Startup the app and login as admin initially to add the sole user that will be serviced by the device.

# update 01/04/2019 by Beitong Tian

This is yueyang version Vegvisir. Added a new strategy "ThingsList" to add block. 
Now we have two strategies to add blocks:

    private enum class strategy{
        BlockchainBrowser,
        ThingsList
    }
    
    //modify this to modify the strategy 
     line 19: private var S:strategy=strategy.ThingsList
     
Do not upgrade gradle if notified.
Password is admin when first time log in. 

here is a blurry video to show how to use it
https://www.youtube.com/embed/Q0jIE7kHlDY

