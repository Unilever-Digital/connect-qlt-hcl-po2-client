## Summary
Local app for streamming data to PBI. Vision Quality SQL Data in line Posimat2.

## Source
```
.
├── README.md                 # README file
├── app   
│   └── controls
│   └── models
│   └── static                # Public folder
│     └── images              # Image used by default template
│         └── png
│         └── jpg
│         └── svg
│     └── css
│     └── js
│   └── templates             # Template ui
│   └── views
│   └── env.py
├── main.py                   # running file
├── .gitignore
├── build                     # Build directory
├── database
├── dist                      # Package app directory
│   └── main
│       └── app
│       └── main.exe
├── package.json
├── Dockerfile                # Dockerfile
├── docker-compose.yml        # docker-compose configuration
└── ...                       # Other configuration files (prettier, ignore files,...)
```
## Requirements:
    requirements.txt

### Virual environment

    - python -m venv venv

    - source venv/bin/activate  # Activate the virtual environment (for Unix-based systems)

    - pip install -r requirements.txt
  
### Build

    - pyinstaller --onefile main.py

### Contributing
Unilever Digital team

### License
This project is licensed under the MIT License. See the LICENSE file for details.

### Contact
For further information or support, please contact the project maintainer: Le-chon-minh.dat@unilever.com

  
