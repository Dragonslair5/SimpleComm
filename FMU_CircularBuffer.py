
from MPI_Constants import *



class FMU_CircularBuffer:

    class CircularBufferInput:

        def __init__(self, match_id: int, size: int):
            self.match_id = match_id;
            self.size = size;
            self.valid = True;

        def invalidate(self):
            assert self.valid;
            self.valid = False;


    def __init__(self, nFMUs: int):
        assert nFMUs > 0;
        self.nFMUs = nFMUs;
        self.circular_buffer = []
        for i in range(0, nFMUs):
            self.circular_buffer.append([])
        self.biggest_buffer_size = 0;

    def insert_entry(self, fmu: int, match_id: int, size: int) -> None:
        assert fmu < self.nFMUs
        newInput = self.CircularBufferInput(match_id, size);
        self.circular_buffer[fmu].append(newInput);


    def get_total_size_on_fmu(self, fmu: int) -> int:
        assert fmu < self.nFMUs
        total_size = 0;
        fmu_buffer: typing.List[self.CircularBufferInput] = self.circular_buffer[fmu];
        for i in range(0, len(fmu_buffer)):
            total_size = total_size + fmu_buffer[i].size;
        if total_size > self.biggest_buffer_size:
            self.biggest_buffer_size = total_size;
        return total_size;

            

    def consume_buffer(self, fmu: int) -> None:
        self.get_total_size_on_fmu(fmu); # We decided to put this here, to calculate the biggest size every time the buffer is consumed.
                                         # To consume the buffer is to move forward the head of the buffer.
        fmu_buffer = self.circular_buffer[fmu];
        assert len(fmu_buffer) > 0;
        while fmu_buffer:
            if not fmu_buffer[0].valid:
                del fmu_buffer[0];
            else:
                break;


    def consume_entry(self, fmu: int, match_id: int):
        fmu_buffer : typing.List[self.CircularBufferInput];
        fmu_buffer = self.circular_buffer[fmu];
        for i in range(0, len(fmu_buffer)):
            if fmu_buffer[i].match_id == match_id:
                fmu_buffer[i].invalidate();
                self.consume_buffer(fmu); # Always try to consume the buffer when an entry is invalidated (consumed)
                return None;
        assert False, "Could not find a match with given ID on given FMU"