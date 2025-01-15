import os
import math
import uuid
import ffmpeg
import logging
from typing import List

def split_video(file_path: str, chunk_size: int) -> List[str]:
    try:
        file_size_bytes = os.path.getsize(file_path)
        chunk_size_bytes = chunk_size * 1024 * 1024
        video_duration = __get_video_duration(file_path)

        # Calculate the play duration from chunk size 
        chunk_duration = math.ceil((chunk_size_bytes / file_size_bytes) * video_duration)

        # Setup chunk file path
        origin_video_name = os.path.basename(file_path).split(".")[0]
        output_dir = f"/tmp/chunks/{origin_video_name}"
        os.makedirs(output_dir, exist_ok=True)

        output_template = os.path.join(output_dir, "%02d.mp4")

        # Calling ffmpeg
        (
            ffmpeg
            .input(file_path)
            .output(
                output_template,
                c="copy",
                f="segment",
                segment_time=chunk_duration,
                reset_timestamps=1,
            )
            .run(capture_stdout=True, capture_stderr=True)
        )

        chunked_files = [os.path.join(output_dir, f) for f in sorted(os.listdir(output_dir)) if f.endswith(".mp4")]
        logging.info(f"Generated {len(chunked_files)} chunks at {output_dir}.")
        return chunked_files
    except Exception as e:
        logging.error(f"Error splitting video: {e}")
        raise

def merge_videos_in_directory(directory_path: str):
    """Merge all MP4 videos in the specified directory into a single MP4 file"""
    try:
        output_path = f"/tmp/{uuid.uuid4().hex}.mp4"
        video_files = [os.path.join(directory_path, f) for f in sorted(os.listdir(directory_path)) if f.endswith(".mp4")]
        if not video_files:
            raise ValueError("No MP4 files found in the directory.")

        logging.info(f"Merging {len(video_files)} video files from {directory_path}.")

        input_file_list = os.path.join(directory_path, "input.txt")
        with open(input_file_list, "w") as f:
            for video_file in video_files:
                f.write(f"file '{video_file}'\n")

        (
            ffmpeg
            .input(input_file_list, format="concat", safe=0)
            .output(output_path, c="copy")
            .run(capture_stdout=True, capture_stderr=True)
        )

        return output_path
    except Exception as e:
        logging.error(f"Failed to merge videos: {e}")
        raise
    finally:
        if input_file_list and os.path.exists(input_file_list):
            os.remove(input_file_list)
        if os.path.exists(directory_path):
            for file in os.listdir(directory_path):
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            os.rmdir(directory_path)

def __get_video_duration(file_path: str) -> float:
    try:
        probe = ffmpeg.probe(file_path)
        duration = float(probe['format']['duration'])
        return duration
    except Exception as e:
        logging.error(f"Error getting video duration: {e}")
        raise

