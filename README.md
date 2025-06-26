# Motion Detection and Video Recording with Raspberry Pi

This project enables a Raspberry Pi to detect motion using a PIR sensor and record a short video clip using a connected USB webcam. It's written in C, uses the WiringPi library for GPIO handling, and leverages `ffmpeg` to handle video recording. Videos are saved with a timestamped filename in the same directory as the executable.

## Features

- Motion detection using GPIO input
- Automatic video recording with timestamped filenames
- Uses `ffmpeg` to capture video from `/dev/video0`
- Saves videos in `.mp4` format silently (no console clutter)

## Requirements

### Hardware

- Raspberry Pi running Linux (e.g., Raspberry Pi OS)
- PIR motion sensor connected to GPIO (see `GPIOwiring.jpg`)
- USB webcam connected (recognized as `/dev/video0`)

### Software

- [WiringPi](http://wiringpi.com/download-and-install/)
- [ffmpeg](https://ffmpeg.org/)

To install `ffmpeg`:

```bash
sudo apt-get update
sudo apt-get install ffmpeg
```

## Installation

1. **Clone the repository**

```bash
git clone https://github.com/yourusername/motion-detection-pi.git
cd motion-detection-pi
```

2. **Compile the C program**

```bash
gcc -o motion_detection motion_detection.c -lwiringPi
```

3. **Run the program (requires root access for GPIO)**

```bash
sudo ./motion_detection
```

## How It Works

- Continuously checks the GPIO pin connected to the PIR sensor.
- When motion is detected:
  - Generates a timestamp
  - Uses `ffmpeg` to record a short video (50 frames at 30 fps)
  - Saves the video using the timestamp as the filename (e.g., `2025-06-26@10:42:31.mp4`)

## Example Output

```
Program is starting ...
Motion detected: Recording video for 10 seconds...
Video saved as 2025-06-26@10:42:31.mp4
```

## Configuration Notes

- The resolution is set to `1280x1720`. You can modify this in the source code to fit your camera.
- Uses MJPEG encoding with quality level `-q:v 2` (high quality).
- Output is silenced using `> /dev/null 2>&1`.
- A 10-second delay follows each recording to avoid duplicate triggers.

## License

This project is provided under the MIT License. See `LICENSE` for details.
