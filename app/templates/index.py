import tkinter as tk
from tkinter import messagebox

# pip install pillow svglib reportlab
from PIL import Image, ImageTk
import cairosvg
import io


class HomeApp(tk.Tk):
    def __init__(self):
        super().__init__()

        # Set window title and size
        self.title("Quality server")
        self.geometry("600x400+200+200")  # Adjust window size as needed

        # Initialize UI elements
        self.init_ui()

    def init_ui(self):
        # Load and display the SVG logo
        self.load_svg_logo("app/static/images/logo.svg")

        # Create the main title label
        self.label_background = tk.Label(self, text="Quality server")
        self.label_background.grid(
            row=1, column=0, columnspan=2, sticky="nsew")

        # Create the privacy label
        self.label_privacy = tk.Label(self, text="Privacy @2023", fg="black")
        self.label_privacy.grid(row=2, column=1, sticky="se", padx=10, pady=10)

        # Configure grid to ensure proper resizing behavior
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=0)
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

    def load_svg_logo(self, svg_path):
        # Convert SVG to PNG using cairosvg
        png_data = cairosvg.svg2png(url=svg_path)

        # Create an in-memory binary stream from the PNG data
        png_stream = io.BytesIO(png_data)

        # Open the PNG image with PIL
        image = Image.open(png_stream)

        # Resize the image as needed (optional)
        logo_width, logo_height = 150, 150  # Adjust dimensions as needed
        image = image.resize((logo_width, logo_height), Image.ANTIALIAS)

        # Convert to Tkinter image
        self.logo_image = ImageTk.PhotoImage(image)

        # Create a label to display the logo
        self.label_logo = tk.Label(self, image=self.logo_image)
        self.label_logo.grid(row=0, column=0, columnspan=2, pady=20)
        self.label_logo.update_idletasks()

        # Center the label
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        
    def close(self, event=None):
        # Confirmation dialog before closing
        if messagebox.askquestion("Quit?", "Are you sure you want to quit?") == "yes":
            super().destroy()  # Close the window