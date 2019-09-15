# Los Santos Obituaries

Repository for a REST service for Twitch chat bots for random obituaries of downed locals

To use, create a new command for your Twitch chat bot of choice.

Usage:
!rip <young|old> <white|black|hispanic> <female|male>

For Nightbot:
$(urlfetch CUSTOM_API_URL_HERE?age=$(1)&race=$(2)&gender=$(3))
For StreamLabs Chatbot:
$readapi(CUSTOM_API_URL_HERE?age=$arg1&race=$arg2&gender=$arg3)
For Deepbot:
@customapi@[CUSTOM_API_URL_HERE?age=@target@[1]&race=@target@[2]&gender=@target@[3]]

Based on @allCalifornians Twitter bot
