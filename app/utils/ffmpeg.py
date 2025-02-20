import os
import math
import uuid
import ffmpeg
import logging

from .tmp_dir_manager import TmpDirManager

def split_video(file_path: str, chunk_size: int) -> TmpDirManager:
    try:
        file_size_bytes = os.path.getsize(file_path)
        chunk_size_bytes = chunk_size * 1024 * 1024
        video_duration = __get_video_duration(file_path)

        # Calculate the play duration from chunk size 
        chunk_duration = math.ceil((chunk_size_bytes / file_size_bytes) * video_duration)

        # Setup chunk file path
        tdm = TmpDirManager()
        output_dir = tdm.get_folder_name()
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

        logging.info(f"Generated {len(tdm.get_files())} chunks at {output_dir}.")
        return tdm
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

def __get_video_duration(file_path: str) -> float:
    try:
        probe = ffmpeg.probe(file_path)
        duration = float(probe['format']['duration'])
        return duration
    except Exception as e:
        logging.error(f"Error getting video duration: {e}")
        raise

