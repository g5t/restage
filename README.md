# restage
Semiautomatic two-stage McCode simulations for parameter scans.

## Motivation
Some instrument simulations are very costly. 
This is especially true for instruments with a large number of components. 
In order to reduce the runtime of a simulated parameter scan, it is possible to run the simulations in two stages.
In the first stage, the instrument is simulated up to a predefined point and the particle states are saved in an MCPL file. 
The output particle states are then used as input for the second stage, 
where the instrument is simulated from the predefined point to the end of the instrument.
On its own, such a split simulation *is detrimental* to the simulation runtime, 
since writing and reading the MCPL file takes time.

Where such a technique is useful, however, is when the first stage output can be reused 
for multiple second stage simulations.
This is the case, e.g., when a 'standard' sample-rotation scan is performed, 
where the sample is rotated around a single axis and the rest of the instrument is kept fixed.
In this case, the first stage simulation only needs to be performed once,
and the second stage simulations can be performed for each sample rotation angle.

## Installation
The `restage` module is installable via `pip`:
```bash
pip install restage
```

or directly from the source code:
```bash
pip install git+https://github.com/g5t/restage.git
```

## Usage

### `splitrun`
The `restage` module provides a command line program, `splitrun`, which
can be used as a replacement for the `mcrun` function distributed as part of McStas.

`splitrun` produces two instrument stages, before and after a split point, and runs
the specified parameter set(s) in both stages connecting the two through cached MCPL
state files.
The first stage results are stored in a user cache directory and the second stage output
is placed under the working directory following the McStas `mcrun` convention.

Command line flags and parameters are mostly the same as used by `mcrun` with a number
of additions to support `splitrun`'s enhanced behavior.
One optional argument, `split-at`, is added; it should be the name of
an `Arm` component inside the instrument file and defaults to `mcpl_split`.
In contrast to `mcrun`, instrument parameters for `splitrun` are specified as 'MATLAB'-range style keyword arguments.
A valid range is inclusive of its end points and of the form `start:step:end` or `start:end` (with implicit `step=1`).

The `-N` argument of `mcrun` is removed since the number of scan steps is now determined by the range
of the scan parameter; this means that multiple scanned parameters must have ranges that agree on the number of steps.

The switch to MATLAB-style range specification is done to allow for flexible mesh scans, via the optional `-m` flag.
When the `-m` flag is specified, the scan parameters are interpreted as mesh scan parameters and the number of steps
along each mesh parameter is not constrained to be the same.

#### Example

As an example, for the instrument file `my_instrument.instr`
```
DEFINE INSTRUMENT my_instrument(sample_angle=0, sample_radius)
COMPONENT source = Source(...) AT (0, 0, 0) ABSOLUTE
COMPONENT guide = GuideGravity(l=10) AT (0, 0, 0.1) RELATIVE source
COMPONENT end_of_guide = Arm() AT (0, 0, 10) RELATIVE guide
COMPONENT slits = Slit(...) AT (0, 0, 0.2) RELATIVE end_of_guide
COMPONENT split_at = Arm() AT (0, 0, 0) RELATIVE slits
COMPONENT sample_pos = Arm() AT (0, 0, 0.2) RELATIVE split_at
COMPONENT sample = Sample(radius=sample_radius) AT (0, 0, 0) RELATIVE sample_pos 
                   ROTATED (0, sample_angle, 0) RELATIVE split_at
COMPONENT detector_arm = Arm() AT (0, 0, 0) RELATIVE sample_pos ROTATED (0, 45, 0) RELATIVE sample_pos
COMPONENT detector = Monitor(...) AT (0, 0, 2) RELATIVE detector_arm
END
```

the McStas `mcrun` command
```bash
mcrun my_instrument.instr -N 90 -n 1000000 -d /data/output sample_angle=1,90 sample_radius=10.0
```
can be replaced by the `splitrun` command
```bash
splitrun my_instrument.instr -n 1000000 --split-at split_at -d /data/output sample_angle=1:90 sample_radius=10.0
```


## MCPL components
There are a small collection of `MCPL` input and output components that are provided
along with the McStas and McXtrace distributions, and you may also decide to write your
own specialized variant of those provided.
By default `splitrun` will insert an `MCPL_output` component at the end of the first
stage, and an `MCPL_input` component at the start of the second stage.
Should you prefer to a different component, or need to provide parameter values for
the components, these can be specified in the command line call to `splitrun`.

Components are specified as their file name (minus the `.comp` extension) and
parameters as comma-separated `key:value` pairs, e.g.:

```bash
splitrun -n 1M a3=0:179 --mcpl-input-component MCPL_input_once --mcpl-output-parameters weight_mode:1,double_prec:1 instr.h5 
```


## Cached data
### Default writable cache
A `sqlite3` database is used to keep track of instrument stages, their compiled
binaries, and output file(s) produced by, e.g., `splitrun` simulations.
The default database location is determined by `platformdirs` under a folder
set by `user_cache_path('restage', 'ess')` and the default locations for 
`restage`-compiled instrument binaries and simulation output is determined from
`user_data_path('restage', 'ess')`.

### Override the database and output locations
These default locations can be overridden by setting the `RESTAGE_CACHE` environment
variable to a writeable folder, e.g., `export RESTAGE_CACHE="/tmp/ephemeral"`.

### Read-only cache database(s)
Any number of fixed databases can be provided to allow for, e.g., system-wide reuse
of common staged simulations.
The location(s) of these database file(s) can be specified as a single
environment variable containing space-separated file locations, e.g.,
`export RESTAGE_FIXED="/usr/local/restage /afs/ess.eu/restage"`.
If the locations provided include a `database.db` file, they will be used to search
for instrument binaries and simulation output directories.

### Use a configuration file to set parameters
Cache configuration information can be provided via a configuration file at, 
e.g., `~/.config/restage/config.yaml`, like
```yaml
cache: /tmp/ephemeral
fixed: /usr/local/restage /afs/ess.eu/restage
```
The exact location searched to find the configuration file is platform dependent,
please consult the [`confuse` documentation](https://confuse.readthedocs.io/en/latest/usage.html)
for the paths used on your system.