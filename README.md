# Humans of Los Santos 

Repository for a REST service for Twitch chat bots and Twitch extension for random biographies of downed locals

To use, create a new command for your Twitch chat bot of choice.

Setup:
Install Humans of Los Santos Twitch extension
Adding the panel is optional

Configure:
Styles

Attributes:
Age: young|old
Race: white|black|hispanic
Gender: male|female


Usage:
!rip attribute1 [attribute2 [attribute3]]

For Nightbot:
$(urlfetch CUSTOM_API_URL_HERE?style=obituary&attributes=$(1),$(2),$(3))
For StreamLabs Chatbot:
$readapi(CUSTOM_API_URL_HERE?style=obituary&attributes=$(1),$(2),$(3))
For Deepbot:
@customapi@[CUSTOM_API_URL_HERE?style=obituary&attributes=@target@[1],@target@[2],@target@[3]]

Based on @allCalifornians Twitter bot
