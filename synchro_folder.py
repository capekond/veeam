# installation:
# alias synchro_folder="python3 /home/ln304/PycharmExt/veenam/synchro_folder.py" NOT WORK

# 1989  crontab -l > mycron
# 1990  echo "*/5 * * * * synchro_folder -a execute -s /home/ln304/Documents -t /home/ln304/Documents_Sync -c # JOB_ID_1" >> mycron
# 1992  crontab mycron
# 1993  crontab -l
# crontab -l > mycron ; echo "*/5 * * * * python3 /home/ln304/PycharmExt/veenam/synchro_folder.py -a execute -s /home/ln304/Documents -t /home/ln304/Documents_Sync -c" >> mycron ; crontab mycron
# python3 /home/ln304/PycharmExt/veenam/synchro_folder.py -a execute -s /home/ln304/Documents -t /home/ln304/Documents_Sync -c


import shutil
import subprocess
import sys
from argparse import ArgumentParser
from hashlib import md5
from os import listdir, path, makedirs, chown, stat, rmdir, remove, system
import logging
from os.path import isdir, isfile
from shutil import copy2
from venv import logger



class SynchroFolder:
    base_folder = ""

    def synchro(self):
        actions = ['start', 'stop', 'list', 'execute']
        parser = ArgumentParser(prog='Synchro', description='Create replica folder and keep it u to data')
        grp1 = parser.add_argument_group('synchro')
        grp1.add_argument("-a", "--action", choices=actions, help="")
        grp1.add_argument("-s", "--source-folder", type=str, help="Source folder")
        grp1.add_argument("-t", "--target-folder", type=str, help="Target folder to be created and synchronized")
        grp1.add_argument("-i", "--interval-min", type=int, help="Interval in minutes from start moment")
        grp1.add_argument("-c", "--no-log-console", action="store_true", help="Disable console logging")
        grp1.add_argument("-l", "--log-level", type=str, default="DEBUG", help="Set logging level")
        parser.add_argument_group('group')
        args = parser.parse_args()
        if (args.action == "execute") and not (args.source_folder and args.target_folder):
            logging.error("Please set --source-folder and --target-folder to execute action")
            exit(1)
        if (args.action == "start") and not (args.source_folder and args.target_folder and args.interval_min):
            logging.error("Please set --source-folder, --target-folder and --interval-min to start action")
            exit(1)
        log_format = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
        log = logging.getLogger()
        log_file = logging.FileHandler("/tmp/synchro.log")
        log_file.setFormatter(log_format)
        log.addHandler(log_file)
        if not args.no_log_console:
            log_console = logging.StreamHandler(sys.stdout)
            log_console.setFormatter(log_format)
            log.addHandler(log_console)
        log.setLevel(args.log_level)

        match args.action:
            case 'start':
                self.start_cron(args.source_folder, args.target_folder, args.interval_min)
            case 'stop':
                self.stop_cron()
            case 'list':
                self.list_cron()
            case 'execute':
                self.execute_sync(args.source_folder, args.target_folder)

    @staticmethod
    def start_cron(source, target, period_min):
        cmd = f'crontab -l > sync_cron ; echo "*/{period_min} * * * * python3 {path.abspath(__file__)} -a execute -s {source} -t {target} -c" >> sync_cron ; crontab sync_cron'
        logger.info(f"Start cron task. Run every {period_min} minutes.")
        system(cmd)

    def stop_cron(self):
        if not self.list_cron():
            return
        approval = input("Are you sure you want to stop cron task? (y/n)")
        if approval == "y":
            try:
                subprocess.check_output('crontab -r', shell=True)
                logger.info("Stopped cron for current user")
            except subprocess.CalledProcessError as e:
                logger.info("No cron task set.")
        else:
            logger.info("Cancel stop cron task.")

    @staticmethod
    def list_cron() -> bool:
        try:
            out = subprocess.check_output('crontab -l', shell=True)
            logger.info(f"Running cron task: {out.decode('utf-8').strip()}")
            return True
        except subprocess.CalledProcessError:
            logger.info("No cron task set")
            return False

    def execute_sync(self, source, target):
        source = source[:-1] if source[-1] == "/" else source
        target = target[:-1] if target[-1] == "/" else target
        logging.info(f"Executing synchro task from {source} to {target}")
        source_structure = self.get_content(source)
        target_structure = self.get_content(target)
        target_add = source_structure - target_structure
        target_remove = target_structure - source_structure
        target_update = 0
        for f in sorted(target_add):
            self.cp_dir_file(source + f, target + f)
        for f in sorted(target_remove, reverse=True):
            self.rm_dir_file(target + f)
        for f in target_structure & source_structure:
            if not (isfile(source + f) == isfile(target + f)):
                self.rm_dir_file(target + f)
                self.cp_dir_file(source + f, target + f)
            if isfile(source + f) and (self.get_md5(source + f) != self.get_md5(target + f)):
                try:
                    logging.debug(f"File {target + f} content will be updated.")
                    self.rm_dir_file(target + f, log=False)
                    self.cp_dir_file(source + f, target + f, log=False)
                    target_update += 1
                except Exception as e:
                    logging.error(f"Cannot update content of the file {target + f} error: {e}")
        logging.info(f"Finished synchro task. Files or directories: Add {len(target_add)}, delete {len(target_remove)}. Updated {target_update} files")

    def get_content(self, folder: str, res=None) -> set:
        if not res:
            res = set()
            self.base_folder = folder
        dir_list = listdir(folder)
        for fd in dir_list:
            ff = path.join(folder, fd)
            res.add(ff[len(self.base_folder):])
            if path.isdir(ff):
                self.get_content(ff, res)
        return res

    @staticmethod
    def rm_dir_file(full_path: str, log=True):
        try:
            if isdir(full_path):
                log and logging.debug(f"Remove directory {full_path} .")
                rmdir(full_path)
            else:
                log and logging.debug(f"Remove file {full_path} from target.")
                remove(full_path)
        except Exception as e:
            logging.error(f"Cannot remove {full_path} error: {e}")

    @staticmethod
    def cp_dir_file(source, target, log=True):
        try:
            if isdir(source):
                log and logging.debug(f"Copy directory {source}  to {target}.")
                makedirs(target)
            else:
                log and logging.debug(f"Copy file {source}  to {target}.")
                shutil.copy2(source, target)
        except Exception as e:
            logging.error(f"Cannot copy {source} to {target} error: {e}")

    @staticmethod
    def get_md5(file_name):
        with open(file_name, 'rb') as f:
            return md5(f.read()).hexdigest()


if __name__ == '__main__':
    sf = SynchroFolder()
    sf.synchro()
