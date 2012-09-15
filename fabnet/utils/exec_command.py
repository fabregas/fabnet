import subprocess

def run_command(args):
    proc = subprocess.Popen(args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    cout, cerr = proc.communicate()

    if proc.returncode:
        raise Exception('"%s" failed with err code %s and message: %s' \
                        %(' '.join(args), proc.returncode, cerr))

    return cout

