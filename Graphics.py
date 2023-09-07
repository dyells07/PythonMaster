import turtle
import time
import json
import urllib.request
import threading

# Constants
API_KEY = 'f0d9f0cb201c6f483554746a721994ba'
WEATHER_URL = 'http://api.openweathermap.org/data/2.5/weather'

wn = turtle.Screen()
wn.title("Traffic Lights & Weather")
wn.bgcolor("black")
# def close_program():
#     wn.bye()

def close_program(x, y):
    if 230 <= x <= 260 and 190 <= y <= 210:
        wn.bye()

close_button = turtle.Turtle()
close_button.speed(0)
close_button.color("red")
close_button.penup()
close_button.goto(230, 190)
close_button.hideturtle()
close_button.write("Close", align="center", font=("Arial", 12, "normal"))

close_button.onclick(close_program)


draw = turtle.Turtle()
draw.color("White")
draw.width(9)
draw.hideturtle()

def draw_traffic_light(color):
    draw.penup()
    draw.goto(0, 20 if color == "red" else -50 if color == "yellow" else -130)
    draw.color("grey" if color == "off" else color)
    draw.dot(60)

# Create a function to get weather data
def get_weather(city):
    try:
        url = f'{WEATHER_URL}?q={city}&appid={API_KEY}'
        response = urllib.request.urlopen(url)
        weather_data = json.loads(response.read())
        return weather_data
    except Exception as e:
        return None

# Function to display weather information
def display_weather(city, weather_info):
    weather_text.clear()
    weather_text.penup()
    weather_text.goto(0, -280)  # Adjust the X and Y coordinates for positioning
    weather_text.write(weather_info, align="center", font=("Arial", 12, "normal"))
    wn.update()

# Create Turtle for Weather Display
weather_text = turtle.Turtle()
weather_text.color("white")
weather_text.penup()
weather_text.hideturtle()

# User input for the city
city = wn.textinput("Enter City", "Enter the name of a city:")
if city is None:
    city = ""

class TrafficLight:
    COLORS = {
        "red": "red",
        "yellow": "yellow",
        "green": "green",
        "grey": "grey"
    }

    def __init__(self):
        self.screen = turtle.Screen()
        self.screen.title("Traffic Lights")
        self.screen.bgcolor("black")

        self.draw_traffic_box()
        self.lights = self.create_lights()
        self.set_initial_colors()

    def draw_traffic_box(self):
        draw.penup()
        draw.goto(-60, 60)
        draw.pendown()
        draw.forward(120)  
        draw.right(90)  
        draw.forward(240)
        draw.right(90)
        draw.forward(120)
        draw.right(90)
        draw.forward(240)

    def create_lights(self):
        lights = {}
        for color, position in [("red", 20), ("yellow", -50), ("green", -130)]:
            lights[color] = self.create_light(color, 0, position)
        return lights

    def create_light(self, color, x, y):
        light = turtle.Turtle(shape="circle")
        light.color(self.COLORS["grey"])
        light.penup()
        light.goto(x, y)
        return light

    def set_initial_colors(self):
        for light in self.lights.values():
            light.color(self.COLORS["grey"])

    def set_color(self, light, color):
        light.color(self.COLORS[color])

    def control_traffic_lights(self, red_duration, yellow_duration, green_duration):
        while True:
            self.set_color(self.lights["red"], "red")
            time.sleep(red_duration)

            self.set_color(self.lights["red"], "grey")
            self.set_color(self.lights["yellow"], "yellow")
            time.sleep(yellow_duration)

            self.set_color(self.lights["yellow"], "grey")
            self.set_color(self.lights["green"], "green")
            time.sleep(green_duration)

            self.set_color(self.lights["green"], "grey")

class WeatherForecast:
    WEATHER_CONDITIONS = ["Sunny", "Cloudy", "Rainy", "Stormy", "Foggy"]

    def __init__(self, city):
        self.city = city

    def get_forecast(self):
        weather_data = get_weather(self.city)
        if weather_data:
            temperature = weather_data['main']['temp']
            humidity = weather_data['main']['humidity']
            pressure = weather_data['main']['pressure']

            return f"Weather in {self.city}:\nTemp: {temperature} K\nHumidity: {humidity}%\nPressure: {pressure} hPa"
        else:
            return f"Weather information for {self.city} not available."

def main():
    traffic_light = TrafficLight()

    try:
        red_duration_str = wn.textinput("Enter Duration", "Enter Red Light Duration (seconds): ")
        yellow_duration_str = wn.textinput("Enter Duration", "Enter Yellow Light Duration (seconds): ")
        green_duration_str = wn.textinput("Enter Duration", "Enter Green Light Duration (seconds): ")
        
        red_duration = int(red_duration_str) if red_duration_str else 3
        yellow_duration = int(yellow_duration_str) if yellow_duration_str else 2
        green_duration = int(green_duration_str) if green_duration_str else 4

        traffic_light_thread = threading.Thread(target=traffic_light.control_traffic_lights, args=(red_duration, yellow_duration, green_duration))

        # Start the traffic light thread
        
        weather_forecast = WeatherForecast(city)
        
        weather_info = weather_forecast.get_forecast()

       
        display_weather(city, weather_info)
        traffic_light_thread.start()

        wn.update()  # Update the screen to display the weather info

    except ValueError:
        print("Please enter valid numeric durations.")

if __name__ == "__main__":
    main()
    wn.mainloop()  # Start the main loop for the turtle graphics
